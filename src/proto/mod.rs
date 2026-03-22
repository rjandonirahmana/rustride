pub mod ridehailing {
    tonic::include_proto!("app");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod order {
    tonic::include_proto!("order");
}

pub mod message {
    tonic::include_proto!("message");
}

pub mod notification {
    tonic::include_proto!("notification");
}
