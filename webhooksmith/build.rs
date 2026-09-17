fn main() {
    println!("cargo:rustc-env=SQLX_OFFLINE=true");
    println!("cargo:rerun-if-changed=.sqlx");
}
