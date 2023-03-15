use std::path::PathBuf;

use freenet_email_inbox::InboxParams;
use locutus_stdlib::prelude::Parameters;
use rsa::{RsaPrivateKey, RsaPublicKey};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

fn main() {
    const RSA_4096_ID_1_PRIV_PEM: &str = include_str!("../../../../web/examples/rsa4096-id-1-priv.pem");
    const RSA_4096_ID_2_PRIV_PEM: &str = include_str!("../../../../web/examples/rsa4096-id-2-priv.pem");

    for key in [(1,RSA_4096_ID_1_PRIV_PEM), (2, RSA_4096_ID_2_PRIV_PEM)] {
        let priv_key =
        <RsaPrivateKey as rsa::pkcs1::DecodeRsaPrivateKey>::from_pkcs1_pem(key.1).unwrap();
        let pub_key = priv_key.to_public_key();
        let inbox_path = PathBuf::from(MANIFEST);
        let params: Parameters = InboxParams { pub_key }
            .try_into()
            .map_err(|e| format!("{e}")).unwrap();
        let params_file_name = format!("inbox_key_{}", key.0);
        std::fs::write(inbox_path.join(params_file_name), params.into_bytes()).unwrap();
    }
}