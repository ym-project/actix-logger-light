use actix_web::{
	Error, Result,
	body::{BodySize, MessageBody},
	dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
	web::Bytes,
};
use pin_project_lite::pin_project;
use std::{
	future::{Future, Ready, ready},
	marker::PhantomData,
	mem,
	pin::Pin,
	task::{Context, Poll, ready},
};
use time::OffsetDateTime;

/// Middleware for logging request and response summaries to the terminal.
/// Fork of [build-in actix-web logger middleware](https://github.com/actix/actix-web/blob/b9d3adfa4d4b70d2a110897adb2207f97e074a77/actix-web/src/middleware/logger.rs).
///
/// Purposes:
/// - remove default error logging. Actix's logger marks errors as DEBUG and there is no way to
///   customize this behavior. See [issue](https://github.com/actix/actix-web/issues/2637). Default
///   log level is INFO for success responses and ERROR for failed responses.
/// - remove formatting in favor of single hardcoded format `%a "%r" %s %b "%{Referer}i" "%{User-Agent}i" %T`
///   output example: `127.0.0.1 "GET /test HTTP/1.1" 404 20 "-" "HTTPie/2.2.0" 0.001074`
///
/// # Examples
/// ```no_run
/// use actix_logger_light::Logger;
/// use actix_web::App;
///
/// // Init logger using env_logger or similar crate before starting the server
/// let app = App::new().wrap(Logger::default());
/// ```
#[derive(Debug, Default)]
pub struct Logger;

impl<S, B> Transform<S, ServiceRequest> for Logger
where
	S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
	B: MessageBody,
{
	type Response = ServiceResponse<StreamLog<B>>;
	type Error = Error;
	type Transform = LoggerMiddleware<S>;
	type InitError = ();
	type Future = Ready<Result<Self::Transform, Self::InitError>>;

	fn new_transform(&self, service: S) -> Self::Future {
		ready(Ok(LoggerMiddleware { service }))
	}
}

/// Logger middleware service.
pub struct LoggerMiddleware<S> {
	service: S,
}

impl<S, B> Service<ServiceRequest> for LoggerMiddleware<S>
where
	S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
	B: MessageBody,
{
	type Response = ServiceResponse<StreamLog<B>>;
	type Error = Error;
	type Future = LoggerResponse<S, B>;

	forward_ready!(service);

	fn call(&self, req: ServiceRequest) -> Self::Future {
		let start_time = OffsetDateTime::now_utc();

		let remote_addr =
			req.connection_info().peer_addr().map(str::to_owned).unwrap_or_else(|| "-".to_owned());

		let request_line = if req.query_string().is_empty() {
			format!("{} {} {:?}", req.method(), req.path(), req.version())
		} else {
			format!("{} {}?{} {:?}", req.method(), req.path(), req.query_string(), req.version())
		};

		let referer =
			req.headers().get("Referer").and_then(|v| v.to_str().ok()).unwrap_or("-").to_owned();

		let user_agent =
			req.headers().get("User-Agent").and_then(|v| v.to_str().ok()).unwrap_or("-").to_owned();

		LoggerResponse {
			fut: self.service.call(req),
			start_time,
			remote_addr,
			request_line,
			referer,
			user_agent,
			_phantom: PhantomData,
		}
	}
}

pin_project! {
	pub struct LoggerResponse<S, B>
	where
		B: MessageBody,
		S: Service<ServiceRequest>,
	{
		#[pin]
		fut: S::Future,
		start_time: OffsetDateTime,
		remote_addr: String,
		request_line: String,
		referer: String,
		user_agent: String,
		_phantom: PhantomData<B>,
	}
}

impl<S, B> Future for LoggerResponse<S, B>
where
	B: MessageBody,
	S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
{
	type Output = Result<ServiceResponse<StreamLog<B>>, Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();

		let res = match ready!(this.fut.poll(cx)) {
			Ok(res) => res,
			Err(err) => return Poll::Ready(Err(err)),
		};

		let is_error = res.response().error().is_some();
		let status = res.status().as_u16();
		let start_time = *this.start_time;
		let remote_addr = mem::take(this.remote_addr);
		let request_line = mem::take(this.request_line);
		let referer = mem::take(this.referer);
		let user_agent = mem::take(this.user_agent);

		Poll::Ready(Ok(res.map_body(move |_, body| StreamLog {
			body,
			size: 0,
			is_error,
			status,
			start_time,
			remote_addr,
			request_line,
			referer,
			user_agent,
		})))
	}
}

pin_project! {
	pub struct StreamLog<B> {
		#[pin]
		body: B,
		size: usize,
		is_error: bool,
		status: u16,
		start_time: OffsetDateTime,
		remote_addr: String,
		request_line: String,
		referer: String,
		user_agent: String,
	}

	impl<B> PinnedDrop for StreamLog<B> {
		fn drop(this: Pin<&mut Self>) {
			let elapsed = OffsetDateTime::now_utc() - this.start_time;
			let elapsed_secs = elapsed.as_seconds_f64();
			let level = if this.is_error { log::Level::Error } else { log::Level::Info };

			// Default target is module_path!()
			log::log!(
				level,
				"{} \"{}\" {} {} \"{}\" \"{}\" {:.6}",
				this.remote_addr,
				this.request_line,
				this.status,
				this.size,
				this.referer,
				this.user_agent,
				elapsed_secs,
			);
		}
	}
}

impl<B: MessageBody> MessageBody for StreamLog<B> {
	type Error = B::Error;

	#[inline]
	fn size(&self) -> BodySize {
		self.body.size()
	}

	fn poll_next(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<Bytes, Self::Error>>> {
		let this = self.project();

		match ready!(this.body.poll_next(cx)) {
			Some(Ok(chunk)) => {
				*this.size += chunk.len();
				Poll::Ready(Some(Ok(chunk)))
			},
			Some(Err(err)) => Poll::Ready(Some(Err(err))),
			None => Poll::Ready(None),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{Logger, StreamLog};
	use actix_web::{
		Error, HttpResponse,
		body::{BoxBody, to_bytes},
		dev::{Service, ServiceRequest, ServiceResponse, Transform, fn_service},
		http::{StatusCode, header},
		test::TestRequest,
	};
	use log::{Level, LevelFilter, Log, Metadata, Record};
	use std::{cell::RefCell, sync::OnceLock};

	// Why thread_local is used?
	// Because variable RECORDS is static and shares state for all test cases. We need to have own
	// state per test case. Because of actix runs every test case in separate thread (using
	// block_on), we use thread_local.
	thread_local! {
		static RECORDS: RefCell<Vec<(Level, String)>> =
			const { RefCell::new(Vec::new()) };
	}

	static LOGGER_INIT: OnceLock<()> = OnceLock::new();

	// Create structure to intercept logs.
	struct CapturingLogger;

	impl Log for CapturingLogger {
		fn enabled(&self, _: &Metadata) -> bool {
			true
		}

		fn log(&self, record: &Record) {
			RECORDS.with(|r| r.borrow_mut().push((record.level(), record.args().to_string())));
		}

		fn flush(&self) {}
	}

	fn setup() {
		LOGGER_INIT.get_or_init(|| {
			log::set_boxed_logger(Box::new(CapturingLogger)).unwrap();
			log::set_max_level(LevelFilter::Trace);
		});
		RECORDS.with(|r| r.borrow_mut().clear());
	}

	fn last_record() -> Option<(Level, String)> {
		RECORDS.with(|r| r.borrow().last().cloned())
	}

	macro_rules! make_srv {
		($status:expr) => {{
			let inner = fn_service(|req: ServiceRequest| async {
				Ok::<_, Error>(req.into_response(HttpResponse::new($status)))
			});
			Logger.new_transform(inner).await.unwrap()
		}};
		($status:expr, $body:expr) => {{
			let inner = fn_service(|req: ServiceRequest| async {
				Ok::<_, Error>(req.into_response(HttpResponse::build($status).body($body)))
			});
			Logger.new_transform(inner).await.unwrap()
		}};
	}

	/// Calls the service, drains the body (triggering PinnedDrop → log), returns the log record.
	async fn call_and_get_log(
		srv: impl Service<ServiceRequest, Response = ServiceResponse<StreamLog<BoxBody>>, Error = Error>,
		req: ServiceRequest,
	) -> (Level, String) {
		setup();
		let res = srv.call(req).await.unwrap();
		to_bytes(res.into_body()).await.unwrap(); // drops StreamLog → PinnedDrop fires
		last_record().expect("no log record captured")
	}

	#[actix_web::test]
	async fn when_response_is_ok() {
		let srv = make_srv!(StatusCode::OK);
		let req = TestRequest::default().to_srv_request();
		let (level, _) = call_and_get_log(srv, req).await;
		assert_eq!(level, Level::Info);
	}

	#[actix_web::test]
	async fn when_response_status_is_404() {
		let srv = make_srv!(StatusCode::NOT_FOUND);
		let req = TestRequest::default().to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(msg.contains("404"), "expected '404' in: {msg}");
	}

	#[actix_web::test]
	async fn when_request_targets_specific_path() {
		let srv = make_srv!(StatusCode::OK);
		let req = TestRequest::get().uri("/api/users").to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(msg.contains("GET /api/users HTTP/1.1"), "expected request line in: {msg}");
	}

	#[actix_web::test]
	async fn when_request_has_query_string() {
		let srv = make_srv!(StatusCode::OK);
		let req = TestRequest::get().uri("/search?q=hello&page=2").to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(
			msg.contains("GET /search?q=hello&page=2 HTTP/1.1"),
			"expected query string in: {msg}"
		);
	}

	#[actix_web::test]
	async fn when_user_agent_is_present() {
		let srv = make_srv!(StatusCode::OK);
		let req = TestRequest::default()
			.insert_header((header::USER_AGENT, "MyBot/2.0"))
			.to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(msg.contains("\"MyBot/2.0\""), "expected user-agent in: {msg}");
	}

	#[actix_web::test]
	async fn when_referer_is_present() {
		let srv = make_srv!(StatusCode::OK);
		let req = TestRequest::default()
			.insert_header((header::REFERER, "https://example.com"))
			.to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(msg.contains("\"https://example.com\""), "expected referer in: {msg}");
	}

	#[actix_web::test]
	async fn when_request_headers_are_absent() {
		let srv = make_srv!(StatusCode::OK);
		let req = TestRequest::default().to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		// Both referer and user-agent should be "-"
		assert!(msg.contains("\"-\" \"-\""), "expected '-' placeholders in: {msg}");
	}

	#[actix_web::test]
	async fn when_request_comes_from_known_peer() {
		let srv = make_srv!(StatusCode::OK);
		let req =
			TestRequest::default().peer_addr("10.0.0.1:4321".parse().unwrap()).to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(msg.contains("10.0.0.1"), "expected peer addr in: {msg}");
	}

	#[actix_web::test]
	async fn when_response_has_body() {
		let srv = make_srv!(StatusCode::OK, "hello"); // 5 bytes
		let req = TestRequest::default().to_srv_request();
		let (_, msg) = call_and_get_log(srv, req).await;
		assert!(msg.contains("200 5"), "expected '200 5' in: {msg}");
	}

	#[actix_web::test]
	async fn when_response_body_is_streamed() {
		let srv = make_srv!(StatusCode::OK, "hello world");
		let req = TestRequest::default().to_srv_request();
		let res = srv.call(req).await.unwrap();
		let body = to_bytes(res.into_body()).await.unwrap();
		assert_eq!(body.as_ref(), b"hello world");
	}
}
