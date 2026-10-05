//! Embedded MVP UI (ui/index.html). Served unauthenticated at /ui so the
//! page can load; it asks for the API token in the browser and sends it as
//! Bearer on every API call (token is kept in sessionStorage only).

pub const INDEX_HTML: &str = include_str!("../../ui/index.html");
