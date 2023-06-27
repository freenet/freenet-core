use std::path::PathBuf;

use identity_management::IdentityParams;
use locutus_stdlib::prelude::Parameters;
use p384::SecretKey;

const HELP: &str = r#"
Options:
 -h --help	Prints this command
 -p --path  	Directory where to save the output (generated params and/or key).
		If not set will use current working directory.
 -k --key 	Key path, if not set one will be generated  
"#;

type DynError = Box<dyn std::error::Error>;
struct Args {
    path: PathBuf,
    key: Option<PathBuf>,
}

impl Args {
    fn parse_args() -> Result<Self, DynError> {
        let mut pargs = pico_args::Arguments::from_env();
        if pargs.contains(["-h", "--help"]) {
            println!("{HELP}");
            std::process::exit(0);
        }
        let path = match pargs.opt_value_from_str::<_, PathBuf>(["-p", "--path"])? {
            Some(p) => p,
            None => std::env::current_dir()?,
        };
        let key = pargs.opt_value_from_str(["-k", "--key"])?;
        if !path.is_dir() {
            return Err("path must be a directory".into());
        }
        Ok(Args { path, key })
    }
}

fn main() -> Result<(), DynError> {
    let args = Args::parse_args()?;

    let secret_key = match args.key {
        Some(path) => {
            let bytes = std::fs::read(path)?;
            SecretKey::from_sec1_pem(&String::from_utf8(bytes)?)?
        }
        None => {
            let mut rng = rand::rngs::OsRng;
            let key = SecretKey::random(&mut rng);
            let as_doc: ecdsa::elliptic_curve::zeroize::Zeroizing<String> = key.to_sec1_pem(p384::pkcs8::LineEnding::LF)?;
            const ID_FILE_NAME: &str = "identity-manager-key";
            println!("storing private key file: `{ID_FILE_NAME}.private.pem`");
            std::fs::write(
                args.path.join(ID_FILE_NAME).with_extension("private.pem"),
                as_doc,
            )?;

            use p384::pkcs8::EncodePublicKey;
            let pub_key = key
                .public_key()
                .to_public_key_der()?
                .to_pem("EC PUBLIC KEY", p384::pkcs8::LineEnding::LF)?;
            println!("storing private key file: `{ID_FILE_NAME}.public.pem`");
            std::fs::write(
                args.path.join(ID_FILE_NAME).with_extension("public.pem"),
                pub_key,
            )?;
            key
        }
    };

    let params: Parameters = IdentityParams { secret_key }.try_into()?;
    const PARAMS_FILE_NAME: &str = "identity-manager-params";
    println!("storing parameters file: `{PARAMS_FILE_NAME}`");
    std::fs::write(args.path.join(PARAMS_FILE_NAME), params.into_bytes())?;
    Ok(())
}
