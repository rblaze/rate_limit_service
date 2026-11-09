#[allow(clippy::result_large_err)]
pub mod envoy {
    pub mod config {
        pub mod core {
            pub mod v3 {
                tonic::include_proto!("envoy.config.core.v3");
            }
        }
    }

    pub mod r#type {
        pub mod v3 {
            tonic::include_proto!("envoy.r#type.v3");
        }
    }

    pub mod extensions {
        pub mod common {
            pub mod ratelimit {
                pub mod v3 {
                    tonic::include_proto!("envoy.extensions.common.ratelimit.v3");
                }
            }
        }
    }

    pub mod service {
        pub mod ratelimit {
            pub mod v3 {
                tonic::include_proto!("envoy.service.ratelimit.v3");
            }
        }
    }
}

pub mod config;
pub mod quota;
pub mod redis_store;
pub mod service;
