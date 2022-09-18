use std::{
    env,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use crate::{
    build::*,
    config::{ContractKind, NewPackageCliConfig},
    util::pipe_std_streams,
    DynError, Error,
};

pub fn create_new_package(config: NewPackageCliConfig) -> Result<(), DynError> {
    let cwd = env::current_dir()?;
    match config.kind {
        ContractKind::WebApp => create_view_package(&cwd)?,
        ContractKind::Contract => create_regular_contract(&cwd)?,
    }
    Ok(())
}

fn create_view_package(cwd: &Path) -> Result<(), DynError> {
    create_rust_crate(cwd, ContractKind::WebApp)?;
    create_web_init_files(cwd)?;
    let locutus_file_config = BuildToolConfig {
        contract: Contract {
            c_type: Some(ContractType::WebApp),
            lang: Some(SupportedContractLangs::Rust),
            output_dir: None,
        },
        webapp: Some(WebAppContract {
            lang: SupportedWebLangs::Typescript,
            typescript: Some(TypescriptConfig { webpack: true }),
            state_sources: Some(Sources {
                source_dirs: Some(vec![PathBuf::from("dist")]),
                files: None,
                output_path: None,
            }),
            metadata: None,
            dependencies: None,
        }),
        state: None,
    };
    let serialized = toml::to_vec(&locutus_file_config)?;
    let path = cwd.join("locutus").with_extension("toml");
    let mut file = File::create(path)?;
    file.write_all(&serialized)?;
    Ok(())
}

fn create_regular_contract(cwd: &Path) -> Result<(), DynError> {
    create_rust_crate(cwd, ContractKind::Contract)?;
    let locutus_file_config = BuildToolConfig {
        contract: Contract {
            c_type: Some(ContractType::Standard),
            lang: Some(SupportedContractLangs::Rust),
            output_dir: None,
        },
        webapp: None,
        state: None,
    };
    let serialized = toml::to_vec(&locutus_file_config)?;
    let path = cwd.join("locutus").with_extension("toml");
    let mut file = File::create(path)?;
    file.write_all(&serialized)?;
    Ok(())
}

fn create_rust_crate(cwd: &Path, kind: ContractKind) -> Result<(), DynError> {
    let (dest_path, cmd) = match kind {
        ContractKind::WebApp => (cwd.join("container"), &["new"]),
        ContractKind::Contract => (cwd.to_owned(), &["init"]),
    };
    let cmd_args = if atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr) {
        cmd.iter()
            .copied()
            .chain(["--color", "always"])
            .chain(["--lib", dest_path.to_str().unwrap()])
            .collect::<Vec<_>>()
    } else {
        cmd.iter()
            .copied()
            .chain(["--lib", dest_path.to_str().unwrap()])
            .collect::<Vec<_>>()
    };

    let child = Command::new("cargo")
        .args(cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing cargo command: {e}");
            Error::CommandFailed("cargo")
        })?;
    pipe_std_streams(child)?;

    // add the stdlib dependency
    let child = Command::new("cargo")
        .args(&["add", "locutus-stdlib"])
        .current_dir(&dest_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing cargo command: {e}");
            Error::CommandFailed("cargo")
        })?;
    pipe_std_streams(child)?;

    // add any additional config keys
    // todo: improve error handling here, in case something fails would have to rollback any changes
    let mut cargo_file = File::open(dest_path.join("Cargo.toml"))?;
    let mut buf = vec![];
    cargo_file.read_to_end(&mut buf)?;
    let mut cargo_def: toml::Value = toml::from_slice(&buf)?;
    let lib_entry = toml::map::Map::from_iter([(
        "crate-type".into(),
        toml::Value::Array(vec![toml::Value::String("cdylib".into())]),
    )]);
    let root = cargo_def.as_table_mut().unwrap();
    root.insert("lib".into(), toml::Value::Table(lib_entry));
    std::mem::drop(cargo_file);
    let mut cargo_file = File::create(dest_path.join("Cargo.toml"))?;
    cargo_file.write_all(&toml::to_vec(&cargo_def)?)?;
    Ok(())
}

fn create_web_init_files(cwd: &Path) -> Result<(), DynError> {
    let child = Command::new("npm")
        .args(&["init", "--force"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(cwd)
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing npm command: {e}");
            Error::CommandFailed("npm")
        })?;
    pipe_std_streams(child)?;
    // todo: change pacakge.json:
    // - include dependencies: locutus-stdlib

    let child = Command::new("tsc")
        .args(&["--init", "--pretty"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(cwd)
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing npm command: {e}");
            Error::CommandFailed("tsc")
        })?;
    pipe_std_streams(child)?;
    // todo: config tsc config file options:
    // - rootDirs: ["./src"]
    // - outDirs: "./dist"

    const WEBPACK_CONFIG: &str = r#"
        const path = require("path");

        module.exports = {
        entry: "./src/index.ts",
        devtool: "inline-source-map",
        output: {
            filename: "bundle.js",
            path: path.resolve(__dirname, "dist"),
        },
        resolve: {
            extensions: [".tsx", ".ts", ".js"],
        },
        devServer: {
            static: path.resolve(__dirname, "dist"),
            port: 8080,
            hot: true,
        },
        module: {
            rules: [
                {
                    test: /\.tsx?$/,
                    use: "ts-loader",
                    exclude: /node_modules/,
                }
            ],
        },
        };"#;

    let mut f = File::create(cwd.join("webpack.config.js"))?;
    f.write_all(WEBPACK_CONFIG.as_bytes())?;

    fs::create_dir_all(cwd.join("src"))?;
    let idx = cwd.join("src").join("index").with_extension("ts");
    File::create(idx)?;

    fs::create_dir_all(cwd.join("dist"))?;
    let idx = cwd.join("dist").join("index").with_extension("html");
    File::create(idx)?;

    Ok(())
}
