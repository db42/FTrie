fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key).ok().as_deref() {
        Some("1") => true,
        Some("0") => false,
        Some("true") | Some("TRUE") | Some("True") => true,
        Some("false") | Some("FALSE") | Some("False") => false,
        None => default,
        _ => default,
    }
}

// Feature flags only.
pub fn is_raft_enabled() -> bool {
    env_bool("RAFT_ENABLED", false)
}

