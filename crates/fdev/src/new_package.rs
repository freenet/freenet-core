use std::{
    collections::BTreeMap,
    env,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    build::*,
    config::{ContractKind, InitPackageConfig},
    util::pipe_std_streams,
    Error,
};

pub fn create_new_package(config: InitPackageConfig) -> anyhow::Result<()> {
    let InitPackageConfig { kind, path } = config;
    let path = path
        .or_else(|| env::current_dir().ok())
        .context("Valid path for creating a new package")?;
    if path.join("freenet.toml").exists() {
        anyhow::bail!(
            "A Freenet project already exists in directory: {}",
            path.display()
        );
    }
    match kind {
        ContractKind::WebApp => create_view_package(&path),
        ContractKind::Contract => create_regular_contract(&path),
    }
}

fn create_view_package(dir: impl AsRef<Path>) -> anyhow::Result<()> {
    let dir = dir.as_ref();
    create_rust_crate(dir, ContractKind::WebApp)?;
    create_web_init_files(dir)?;
    let freenet_file_config = ContractBuildConfig {
        contract: Contract {
            c_type: Some(ContractType::WebApp),
            lang: Some(SupportedContractLangs::Rust),
            output_dir: None,
        },
        webapp: Some(WebAppContract {
            lang: Some(SupportedWebLangs::Typescript),
            typescript: Some(TypescriptConfig { webpack: true }),
            state_sources: Sources {
                source_dirs: Some(vec![PathBuf::from("dist")]),
                files: None,
            },
            metadata: None,
            dependencies: None,
        }),
        state: None,
    };
    let serialized = toml::to_string(&freenet_file_config)?.into_bytes();
    let path = dir.join("freenet").with_extension("toml");
    let mut file = File::create(path)?;
    file.write_all(&serialized)?;
    Ok(())
}

fn create_regular_contract(dir: impl AsRef<Path>) -> anyhow::Result<()> {
    let dir = dir.as_ref();
    create_rust_crate(dir, ContractKind::Contract)?;
    let freenet_file_config = ContractBuildConfig {
        contract: Contract {
            c_type: Some(ContractType::Standard),
            lang: Some(SupportedContractLangs::Rust),
            output_dir: None,
        },
        webapp: None,
        state: None,
    };
    let serialized = toml::to_string(&freenet_file_config)?.into_bytes();
    let path = dir.join("freenet").with_extension("toml");
    let mut file = File::create(path)?;
    file.write_all(&serialized)?;
    Ok(())
}

/// Minimal Cargo.toml that only contains fields we need.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct Manifest {
    package: toml::Value,
    dependencies: toml::Value,
    #[serde(default)]
    lib: Option<ManifestLib>,
    #[serde(default)]
    features: Option<BTreeMap<String, Vec<String>>>,
    // Catch-all to ensure we don't drop any fields.
    #[serde(default, flatten)]
    any: Option<BTreeMap<String, toml::Value>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ManifestLib {
    name: Option<String>,
    path: Option<PathBuf>,
    crate_type: Option<Vec<String>>,
    required_features: Option<Vec<String>>,
}

fn create_rust_crate(dir: impl AsRef<Path>, kind: ContractKind) -> anyhow::Result<()> {
    let dir = match kind {
        ContractKind::WebApp => dir.as_ref().join("container"),
        ContractKind::Contract => dir.as_ref().to_owned(),
    };
    let command = if dir.exists() { "init" } else { "new" };
    let colored = [command, "--color", "always", "--lib"];
    let auto_colored = [command, "--lib"];
    use std::io::IsTerminal;
    let cmd_args = if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
        colored
            .as_slice()
            .iter()
            .copied()
            .map(std::ffi::OsStr::new)
            .chain([dir.as_os_str()])
    } else {
        auto_colored
            .as_slice()
            .iter()
            .copied()
            .map(std::ffi::OsStr::new)
            .chain([dir.as_os_str()])
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
        .args(["add", "freenet-stdlib"])
        .current_dir(&dir)
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
    let manifest_path = dir.join("Cargo.toml");
    let toml_str = fs::read_to_string(&manifest_path)?;
    let mut manifest: Manifest = toml::from_str(&toml_str)?;

    manifest.lib = Some(ManifestLib {
        crate_type: Some(vec!["cdylib".into()]),
        ..Default::default()
    });
    manifest.features = Some(
        [
            ("default".into(), vec!["freenet-main-contract".into()]),
            ("contract".into(), vec!["freenet-stdlib/contract".into()]),
            ("freenet-main-contract".into(), vec![]),
            ("trace".into(), vec!["freenet-stdlib/trace".into()]),
        ]
        .into_iter()
        .collect(),
    );

    fs::write(&manifest_path, toml::to_string(&manifest)?)?;
    Ok(())
}

#[cfg(windows)]
const NPM: &str = "npm.cmd";
#[cfg(unix)]
const NPM: &str = "npm";

#[cfg(windows)]
const TSC: &str = "tsc.cmd";
#[cfg(unix)]
const TSC: &str = "tsc";

fn create_web_init_files(dir: impl AsRef<Path>) -> anyhow::Result<()> {
    let dir = dir.as_ref();
    let child = Command::new(NPM)
        .args(["init", "--force"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(dir)
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing npm command: {e}");
            Error::CommandFailed("npm")
        })?;
    pipe_std_streams(child)?;
    // todo: change package.json:
    // - include dependencies: freenet-stdlib

    let child = Command::new(TSC)
        .args(["--init", "--pretty"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(dir)
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

    let mut f = File::create(dir.join("webpack.config.js"))?;
    f.write_all(WEBPACK_CONFIG.as_bytes())?;

    fs::create_dir_all(dir.join("src"))?;
    let idx = dir.join("src").join("index").with_extension("ts");
    File::create(idx)?;

    fs::create_dir_all(dir.join("dist"))?;
    let idx = dir.join("dist").join("index").with_extension("html");
    File::create(idx)?;

    Ok(())
}
