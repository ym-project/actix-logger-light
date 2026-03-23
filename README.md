# Actix logger light

Fork of [build-in actix-web logger middleware](https://github.com/actix/actix-web/blob/b9d3adfa4d4b70d2a110897adb2207f97e074a77/actix-web/src/middleware/logger.rs).
Purposes:

- remove default error logging. Actix's logger marks errors as DEBUG and there is no way to customize this behavior. See [issue](https://github.com/actix/actix-web/issues/2637)
- remove formatting in favor of single hardcoded format `%a "%r" %s %b "%{Referer}i" "%{User-Agent}i" %T`
  output example: `127.0.0.1 "GET /test HTTP/1.1" 404 20 "-" "HTTPie/2.2.0" 0.001074`

Use it if you want to speed up your server a little and don't need to customize log output.

## Installation

```sh
cargo add actix-logger-light
```

## Example

```rs
use actix_web::{App};
use actix_logger_light::Logger;

// Init logger using env_logger or similar crate
env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

let app = App::new().wrap(Logger::default());
```

## License

This project is licensed under of MIT license ([LICENSE](LICENSE) or [https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT))
