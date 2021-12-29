// buffermanager は dao で公開した I/F をサポートする diskmanager を使うことができる
pub mod dao;

// Clock-sweek を使った buffer pool による buffermanager の具体的な実装
pub mod simple;
