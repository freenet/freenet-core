//! Acceptance rule for WASM code offered to the compiler.
//!
//! The scan is a full `wasmparser` validation (linear in the module size) that
//! counts functions, function body size, locals, control frames and their
//! nesting, branch edges, `br_table` targets and block-type arity. Every limit
//! is checked while the walk proceeds, so a module over a limit is refused
//! without finishing the walk, and nothing is compiled until the whole module
//! has been accepted. The scan accepts exactly the WebAssembly proposals in
//! [`accepted_features`]; the engine is configured with the same set, so a
//! module using any other proposal is refused as invalid.
//!
//! Each limit sits well above the largest value measured over the contracts
//! and delegates deployed on the network; the measurements are in
//! `reference_shapes.json` next to this file and the test
//! `reference_module_shapes_stay_inside_every_limit` keeps the two in step. The
//! limits are a network-wide acceptance rule: a node that runs a build with
//! different limits accepts different code, so a change to one is a
//! rolling-upgrade event.
//!
//! The scan is a coarse pre-filter, not a bound on the cost of compiling what
//! it accepts. That bound is the compile worker's CPU, memory and wall limits
//! (`compile_worker`).
//!
//! This module uses `wasmparser` only, so it holds for any engine behind
//! [`super::WasmEngine`].

use std::fmt::{self, Write as _};

use serde::{Deserialize, Serialize};
use wasmparser::{
    BlockType, CompositeInnerType, FuncValidatorAllocations, Parser, Payload, ValidPayload,
    Validator, WasmFeatures, WasmModuleResources,
};

/// Largest module accepted, in bytes. Matches `native_api::MAX_WASM_CODE_SIZE`.
pub(crate) const MAX_MODULE_BYTES: usize = 10 * 1024 * 1024;
/// Most functions a module may define.
pub(crate) const MAX_FUNCTIONS: u32 = 34_200;
/// Largest single function body, in bytes.
pub(crate) const MAX_FUNCTION_BODY_BYTES: usize = 920 * 1024;
/// Most locals plus parameters in one function.
pub(crate) const MAX_FUNCTION_LOCALS: u32 = 4_300;
/// Per-function bound on `(locals + params) * body bytes`.
pub(crate) const MAX_FUNCTION_LOCALS_TIMES_BODY: u64 = 52 * 1024 * 1024;
/// Most control frames (`block`, `loop`, `if`, `try`, `try_table`) in one function.
pub(crate) const MAX_FUNCTION_CONTROL_FRAMES: u32 = 22_700;
/// Per-function bound on `(locals + params) * control frames`.
pub(crate) const MAX_FUNCTION_LOCALS_TIMES_FRAMES: u64 = 1_000_000;
/// The same product summed over every function of the module.
pub(crate) const MAX_MODULE_LOCALS_TIMES_FRAMES: u64 = 5_900_000;
/// Bound on the sum, over functions, of `control frames squared`.
pub(crate) const MAX_MODULE_CONTROL_FRAMES_SQUARED: u64 = 156_000_000;
/// Most control frames in the whole module.
pub(crate) const MAX_MODULE_CONTROL_FRAMES: u32 = 211_000;
/// Most branch edges in one function: every `br`, `br_if`, `br_on_*`, `if`,
/// `try_table` catch clause, and each target of a `br_table` (its default
/// included).
pub(crate) const MAX_FUNCTION_BRANCH_EDGES: u32 = 41_400;
/// Bound on the sum, over functions, of `branch edges squared`.
pub(crate) const MAX_MODULE_BRANCH_EDGES_SQUARED: u64 = 1_800_000_000;
/// Deepest control-frame nesting in one function.
pub(crate) const MAX_NESTING_DEPTH: u32 = 1_024;
/// Most targets in one `br_table`.
pub(crate) const MAX_BR_TABLE_TARGETS: u32 = 4_096;
/// Most params plus results of a type-indexed block.
pub(crate) const MAX_BLOCK_TYPE_ARITY: u32 = 32;
/// Per-function bound on the sum of type-indexed block arities.
pub(crate) const MAX_BLOCK_TYPE_ARITY_SUM: u64 = 4_096;
/// Most parameters of a function type.
pub(crate) const MAX_FN_TYPE_PARAMS: u32 = 96;
/// Most results of a function type.
pub(crate) const MAX_FN_TYPE_RESULTS: u32 = 16;
/// Tallest operand stack in one function.
pub(crate) const MAX_OPERAND_STACK_HEIGHT: u32 = 137;
/// Most types in the type section (each member of a recursion group counts).
pub(crate) const MAX_TYPE_ENTRIES: u32 = 10_000;
/// Bound on the operator count of one function (every operator counts 1).
pub(crate) const MAX_FUNCTION_OPS: u64 = 410_000;
/// Bound on the operator count of the whole module.
pub(crate) const MAX_MODULE_OPS: u64 = 5_100_000;
/// Bound on the cost-weighted operator count of one function (see
/// [`operator_weight`]).
pub(crate) const MAX_FUNCTION_WEIGHTED_OPS: u64 = 8_400_000;
/// Bound on the cost-weighted operator count of the whole module.
pub(crate) const MAX_MODULE_WEIGHTED_OPS: u64 = 123_000_000;
/// Bound on the sum, over functions, of `weighted operators * max(1, frames)`.
/// Frames here include the block-creating operators (see [`creates_blocks`]).
/// This is the generic backstop for operator classes the model does not name.
pub(crate) const MAX_MODULE_OPS_TIMES_FRAMES: u64 = 57_100_000_000;
/// Bound on the sum, over functions, of `(locals + params) * branch edges`.
pub(crate) const MAX_MODULE_LOCALS_TIMES_EDGES: u64 = 9_500_000;
/// Bound on the sum, over functions, of `weighted operators * (locals + params)`:
/// compile work grows with both.
pub(crate) const MAX_MODULE_OPS_TIMES_LOCALS: u64 = 3_500_000_000;
/// Bound on the sum, over every function of the module, of the parameters and
/// results of its typed blocks.
pub(crate) const MAX_MODULE_BLOCK_TYPE_ARITY: u64 = 8_192;
/// Bound on the entries of all element segments (active, passive and
/// declarative, function-index and expression forms) of the module. It is far
/// below wasmtime's own static-initialisation limit.
pub(crate) const MAX_MODULE_ELEMENT_ENTRIES: u64 = 2_500;
/// Bound on the initial entries of all tables of the module.
pub(crate) const MAX_MODULE_TABLE_ENTRIES: u64 = 2_500;
/// Bound on the number of data segments.
pub(crate) const MAX_MODULE_DATA_SEGMENTS: u64 = 750;
/// Bound on the bytes of all data segments.
pub(crate) const MAX_MODULE_DATA_TOTAL: u64 = MAX_MODULE_BYTES as u64;
/// Bound on the sum, over functions, of `locals + params`.
pub(crate) const MAX_MODULE_LOCALS: u64 = 192_000;
/// Bound on the sum, over functions, of `weighted operators * tallest operand
/// stack`: compile work grows with both.
pub(crate) const MAX_MODULE_OPS_TIMES_STACK: u64 = 844_000_000;
/// Bound on the sum, over functions, of the product of the branch edges, the
/// deepest control nesting and the locals plus parameters. Declared locals are
/// used rather than modified ones (conservative, and it needs no data-flow
/// pass).
pub(crate) const MAX_MODULE_EDGES_TIMES_DEPTH_TIMES_LOCALS: u64 = 637_000_000;
/// Bound on the functions (imported and defined) a module has.
pub(crate) const MAX_MODULE_FUNCTIONS_TOTAL: u64 = 34_200;
/// Bound on the functions a module exports; each keeps a call trampoline.
pub(crate) const MAX_MODULE_EXPORTED_FUNCTIONS: u64 = 17_000;
/// Bound on the sum, over every function (imported and defined), of the
/// parameters plus results of its type: each one gets a call trampoline.
pub(crate) const MAX_MODULE_FN_ARITY: u64 = 92_000;
/// Bound on the sum, over every function type, of its parameters plus results.
pub(crate) const MAX_MODULE_TYPE_ARITY: u64 = 1_900;
/// Longest diagnostic text kept from a parser or compiler error, in bytes.
pub(crate) const MAX_DIAGNOSTIC_BYTES: usize = 256;

/// The WebAssembly proposals contract and delegate code may use. Everything
/// else is refused as invalid by the scan, and the engine is configured with
/// this same set (see `WasmtimeEngine::create_engine`).
///
/// The list is the WebAssembly 2.0 feature set. Every module in the corpus
/// behind `reference_shapes.json` validates with it, and none uses a later
/// proposal (exceptions, GC, function references, tail calls, threads, 64-bit or
/// multiple memories, relaxed SIMD, extended constants, wide arithmetic). Adding
/// a proposal is a change to the acceptance rule, not a tuning step.
pub(crate) fn accepted_features() -> WasmFeatures {
    WasmFeatures::MUTABLE_GLOBAL
        | WasmFeatures::SATURATING_FLOAT_TO_INT
        | WasmFeatures::SIGN_EXTENSION
        | WasmFeatures::REFERENCE_TYPES
        | WasmFeatures::MULTI_VALUE
        | WasmFeatures::BULK_MEMORY
        | WasmFeatures::SIMD
        | WasmFeatures::FLOATS
}

/// `value` as text, cut to at most [`MAX_DIAGNOSTIC_BYTES`] bytes (on a
/// character boundary, with an ellipsis when cut). Formatting stops as soon as
/// the budget is spent, so an error whose text is as large as its input never
/// becomes a large string here.
pub(crate) fn bounded_text(value: &dyn fmt::Display) -> String {
    struct Budget(String);
    impl fmt::Write for Budget {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            // Leave room for the ellipsis so the whole text stays within the bound.
            let room = MAX_DIAGNOSTIC_BYTES - 3 - self.0.len();
            if s.len() <= room {
                self.0.push_str(s);
                return Ok(());
            }
            let mut cut = room;
            while !s.is_char_boundary(cut) {
                cut -= 1;
            }
            self.0.push_str(&s[..cut]);
            Err(fmt::Error)
        }
    }
    let mut budget = Budget(String::new());
    if write!(budget, "{value}").is_err() {
        budget.0.push_str("...");
    }
    budget.0
}

/// Largest value of each counted quantity in a module (a module's "shape").
/// Field names are the keys of `reference_shapes.json`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ModuleShape {
    pub(crate) module_bytes: u64,
    pub(crate) functions: u64,
    pub(crate) body_bytes_per_fn: u64,
    pub(crate) locals_plus_params_per_fn: u64,
    pub(crate) locals_times_body: u64,
    pub(crate) control_frames_per_fn: u64,
    pub(crate) control_frames_per_module: u64,
    pub(crate) control_frames_squared_per_module: u64,
    pub(crate) locals_times_frames_per_fn: u64,
    pub(crate) locals_times_frames_per_module: u64,
    pub(crate) branch_edges_per_fn: u64,
    pub(crate) branch_edges_squared_per_module: u64,
    pub(crate) nesting_depth: u64,
    pub(crate) br_table_targets: u64,
    pub(crate) block_type_arity: u64,
    pub(crate) block_type_arity_sum_per_fn: u64,
    pub(crate) fn_type_params: u64,
    pub(crate) fn_type_results: u64,
    pub(crate) operand_stack_height: u64,
    pub(crate) type_entries: u64,
    pub(crate) ops_per_fn: u64,
    pub(crate) weighted_ops_per_fn: u64,
    pub(crate) weighted_ops_per_module: u64,
    pub(crate) locals_per_module: u64,
    pub(crate) ops_times_stack_per_module: u64,
    pub(crate) edges_times_depth_times_locals_per_module: u64,
    pub(crate) functions_total: u64,
    pub(crate) exported_functions: u64,
    pub(crate) block_type_arity_per_module: u64,
    pub(crate) element_entries: u64,
    pub(crate) table_entries: u64,
    pub(crate) data_segments: u64,
    pub(crate) data_bytes: u64,
    pub(crate) ops_times_locals_per_module: u64,
    pub(crate) ops_per_module: u64,
    pub(crate) ops_times_frames_per_module: u64,
    pub(crate) locals_times_edges_per_module: u64,
    pub(crate) fn_arity_per_module: u64,
    pub(crate) type_arity_per_module: u64,
}

impl ModuleShape {
    /// The limit for each field.
    pub(crate) fn limits() -> ModuleShape {
        ModuleShape {
            module_bytes: MAX_MODULE_BYTES as u64,
            functions: MAX_FUNCTIONS.into(),
            body_bytes_per_fn: MAX_FUNCTION_BODY_BYTES as u64,
            locals_plus_params_per_fn: MAX_FUNCTION_LOCALS.into(),
            locals_times_body: MAX_FUNCTION_LOCALS_TIMES_BODY,
            control_frames_per_fn: MAX_FUNCTION_CONTROL_FRAMES.into(),
            control_frames_per_module: MAX_MODULE_CONTROL_FRAMES.into(),
            control_frames_squared_per_module: MAX_MODULE_CONTROL_FRAMES_SQUARED,
            locals_times_frames_per_fn: MAX_FUNCTION_LOCALS_TIMES_FRAMES,
            locals_times_frames_per_module: MAX_MODULE_LOCALS_TIMES_FRAMES,
            branch_edges_per_fn: MAX_FUNCTION_BRANCH_EDGES.into(),
            branch_edges_squared_per_module: MAX_MODULE_BRANCH_EDGES_SQUARED,
            nesting_depth: MAX_NESTING_DEPTH.into(),
            br_table_targets: MAX_BR_TABLE_TARGETS.into(),
            block_type_arity: MAX_BLOCK_TYPE_ARITY.into(),
            block_type_arity_sum_per_fn: MAX_BLOCK_TYPE_ARITY_SUM,
            fn_type_params: MAX_FN_TYPE_PARAMS.into(),
            fn_type_results: MAX_FN_TYPE_RESULTS.into(),
            operand_stack_height: MAX_OPERAND_STACK_HEIGHT.into(),
            type_entries: MAX_TYPE_ENTRIES.into(),
            ops_per_fn: MAX_FUNCTION_OPS,
            weighted_ops_per_fn: MAX_FUNCTION_WEIGHTED_OPS,
            weighted_ops_per_module: MAX_MODULE_WEIGHTED_OPS,
            locals_per_module: MAX_MODULE_LOCALS,
            ops_times_stack_per_module: MAX_MODULE_OPS_TIMES_STACK,
            edges_times_depth_times_locals_per_module: MAX_MODULE_EDGES_TIMES_DEPTH_TIMES_LOCALS,
            functions_total: MAX_MODULE_FUNCTIONS_TOTAL,
            exported_functions: MAX_MODULE_EXPORTED_FUNCTIONS,
            block_type_arity_per_module: MAX_MODULE_BLOCK_TYPE_ARITY,
            element_entries: MAX_MODULE_ELEMENT_ENTRIES,
            table_entries: MAX_MODULE_TABLE_ENTRIES,
            data_segments: MAX_MODULE_DATA_SEGMENTS,
            data_bytes: MAX_MODULE_DATA_TOTAL,
            ops_times_locals_per_module: MAX_MODULE_OPS_TIMES_LOCALS,
            ops_per_module: MAX_MODULE_OPS,
            ops_times_frames_per_module: MAX_MODULE_OPS_TIMES_FRAMES,
            locals_times_edges_per_module: MAX_MODULE_LOCALS_TIMES_EDGES,
            fn_arity_per_module: MAX_MODULE_FN_ARITY,
            type_arity_per_module: MAX_MODULE_TYPE_ARITY,
        }
    }
}

/// Digest of every limit, of the accepted feature set and of the engine that
/// compiles (`engine_id`, see `wasmtime_engine::engine_compat_id`: it covers the
/// wasmtime version and the engine configuration). A remembered refusal is keyed
/// by it, so a release that changes a limit, or moves to another wasmtime, does
/// not answer from a refusal recorded under other limits. It is computed from
/// [`ModuleShape::limits`], which has one field per limit, so a new limit is in
/// the digest without anyone remembering to add it.
pub(crate) fn limits_fingerprint(engine_id: u64) -> [u8; 32] {
    fingerprint_of(&ModuleShape::limits(), engine_id)
}

/// The operator model as text: its version and the three weights.
fn operator_model() -> String {
    format!("{OPERATOR_MODEL_VERSION}|{WEIGHT_CHEAP},{WEIGHT_COMMON},{WEIGHT_HEAVY}")
}

fn fingerprint_of(limits: &ModuleShape, engine_id: u64) -> [u8; 32] {
    fingerprint_with_model(limits, engine_id, &operator_model())
}

fn fingerprint_with_model(limits: &ModuleShape, engine_id: u64, model: &str) -> [u8; 32] {
    *blake3::hash(format!("{limits:?}|{:?}|{engine_id}|{model}", accepted_features()).as_bytes())
        .as_bytes()
}

/// A counter is reported when a module that passes the scan reaches this
/// percentage of its limit.
pub(crate) const NEAR_LIMIT_PERCENT: u64 = 50;

/// The counter of `shape` closest to its limit, if it is at or above
/// [`NEAR_LIMIT_PERCENT`] of that limit: `(name, value, limit)`.
pub(crate) fn nearest_limit(shape: &ModuleShape) -> Option<(&'static str, u64, u64)> {
    let (v, l) = (shape, ModuleShape::limits());
    let rows: [(&'static str, u64, u64); 39] = [
        ("module_bytes", v.module_bytes, l.module_bytes),
        ("functions", v.functions, l.functions),
        (
            "body_bytes_per_fn",
            v.body_bytes_per_fn,
            l.body_bytes_per_fn,
        ),
        (
            "locals_plus_params_per_fn",
            v.locals_plus_params_per_fn,
            l.locals_plus_params_per_fn,
        ),
        (
            "locals_times_body",
            v.locals_times_body,
            l.locals_times_body,
        ),
        (
            "control_frames_per_fn",
            v.control_frames_per_fn,
            l.control_frames_per_fn,
        ),
        (
            "control_frames_per_module",
            v.control_frames_per_module,
            l.control_frames_per_module,
        ),
        (
            "control_frames_squared_per_module",
            v.control_frames_squared_per_module,
            l.control_frames_squared_per_module,
        ),
        (
            "locals_times_frames_per_fn",
            v.locals_times_frames_per_fn,
            l.locals_times_frames_per_fn,
        ),
        (
            "locals_times_frames_per_module",
            v.locals_times_frames_per_module,
            l.locals_times_frames_per_module,
        ),
        (
            "branch_edges_per_fn",
            v.branch_edges_per_fn,
            l.branch_edges_per_fn,
        ),
        (
            "branch_edges_squared_per_module",
            v.branch_edges_squared_per_module,
            l.branch_edges_squared_per_module,
        ),
        ("nesting_depth", v.nesting_depth, l.nesting_depth),
        ("br_table_targets", v.br_table_targets, l.br_table_targets),
        ("block_type_arity", v.block_type_arity, l.block_type_arity),
        (
            "block_type_arity_sum_per_fn",
            v.block_type_arity_sum_per_fn,
            l.block_type_arity_sum_per_fn,
        ),
        ("fn_type_params", v.fn_type_params, l.fn_type_params),
        ("fn_type_results", v.fn_type_results, l.fn_type_results),
        (
            "operand_stack_height",
            v.operand_stack_height,
            l.operand_stack_height,
        ),
        ("type_entries", v.type_entries, l.type_entries),
        ("ops_per_fn", v.ops_per_fn, l.ops_per_fn),
        (
            "weighted_ops_per_fn",
            v.weighted_ops_per_fn,
            l.weighted_ops_per_fn,
        ),
        (
            "weighted_ops_per_module",
            v.weighted_ops_per_module,
            l.weighted_ops_per_module,
        ),
        (
            "ops_times_locals_per_module",
            v.ops_times_locals_per_module,
            l.ops_times_locals_per_module,
        ),
        (
            "block_type_arity_per_module",
            v.block_type_arity_per_module,
            l.block_type_arity_per_module,
        ),
        ("element_entries", v.element_entries, l.element_entries),
        ("table_entries", v.table_entries, l.table_entries),
        ("data_segments", v.data_segments, l.data_segments),
        ("data_bytes", v.data_bytes, l.data_bytes),
        (
            "ops_times_stack_per_module",
            v.ops_times_stack_per_module,
            l.ops_times_stack_per_module,
        ),
        (
            "edges_times_depth_times_locals_per_module",
            v.edges_times_depth_times_locals_per_module,
            l.edges_times_depth_times_locals_per_module,
        ),
        ("functions_total", v.functions_total, l.functions_total),
        (
            "exported_functions",
            v.exported_functions,
            l.exported_functions,
        ),
        (
            "locals_per_module",
            v.locals_per_module,
            l.locals_per_module,
        ),
        ("ops_per_module", v.ops_per_module, l.ops_per_module),
        (
            "ops_times_frames_per_module",
            v.ops_times_frames_per_module,
            l.ops_times_frames_per_module,
        ),
        (
            "locals_times_edges_per_module",
            v.locals_times_edges_per_module,
            l.locals_times_edges_per_module,
        ),
        (
            "fn_arity_per_module",
            v.fn_arity_per_module,
            l.fn_arity_per_module,
        ),
        (
            "type_arity_per_module",
            v.type_arity_per_module,
            l.type_arity_per_module,
        ),
    ];
    rows.into_iter()
        .filter(|(_, value, limit)| {
            value.saturating_mul(100) >= limit.saturating_mul(NEAR_LIMIT_PERCENT)
        })
        // Compare value/limit as cross products to stay in integers.
        .max_by(|a, b| (a.1 as u128 * b.2 as u128).cmp(&(b.1 as u128 * a.2 as u128)))
}

/// Why a module was refused, one variant per limit. Every variant is a property
/// of the module bytes, so a refusal is permanent for that module.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CompileRefusal {
    #[error("module is {size} bytes, over the {limit} byte limit")]
    ModuleTooLarge { size: usize, limit: usize },
    /// The text is cut to [`MAX_DIAGNOSTIC_BYTES`] (see [`bounded_text`]).
    #[error("module is not WebAssembly binary format: {0}")]
    NotBinary(String),
    /// Invalid for the accepted feature set, or malformed. The text is cut to
    /// [`MAX_DIAGNOSTIC_BYTES`] (see [`bounded_text`]).
    #[error("module is invalid: {0}")]
    Malformed(String),
    #[error("module defines more than {limit} functions")]
    TooManyFunctions { limit: u32 },
    #[error("a function body is over the {limit} byte limit")]
    FunctionTooLarge { limit: usize },
    #[error("a function has more than {limit} locals and parameters")]
    TooManyLocals { limit: u32 },
    #[error("a function's locals times body size is over the {limit} limit")]
    LocalsTimesBodyTooLarge { limit: u64 },
    #[error("a function has more than {limit} control frames")]
    TooManyControlFrames { limit: u32 },
    #[error("a function has more than {limit} branch edges")]
    TooManyBranchEdges { limit: u32 },
    #[error("the module's branch edges squared, summed over functions, is over {limit}")]
    ModuleBranchEdgesSquaredTooLarge { limit: u64 },
    #[error("a function's locals times control frames is over {limit}")]
    LocalsTimesFramesTooLarge { limit: u64 },
    #[error("the module's locals times control frames, summed over functions, is over {limit}")]
    ModuleLocalsTimesFramesTooLarge { limit: u64 },
    #[error("the module's control frames squared, summed over functions, is over {limit}")]
    ModuleControlFramesSquaredTooLarge { limit: u64 },
    #[error("module has more than {limit} control frames")]
    TooManyModuleControlFrames { limit: u32 },
    #[error("control frames nest deeper than {limit}")]
    NestingTooDeep { limit: u32 },
    #[error("a br_table has more than {limit} targets")]
    BrTableTooLarge { limit: u32 },
    #[error("a block type has more than {limit} params and results")]
    BlockArityTooLarge { limit: u32 },
    #[error("a function's block type arities sum to more than {limit}")]
    BlockArityTotalTooLarge { limit: u64 },
    #[error("a function type has more than {limit} parameters or {results} results")]
    FunctionTypeTooLarge { limit: u32, results: u32 },
    #[error("the operand stack grows taller than {limit}")]
    OperandStackTooTall { limit: u32 },
    #[error("module has more than {limit} types")]
    TooManyTypes { limit: u32 },
    #[error("a function has more than {limit} operators")]
    TooManyOps { limit: u64 },
    #[error("a function's operators weigh more than {limit} in total")]
    TooManyWeightedOps { limit: u64 },
    #[error("the module's operators weigh more than {limit} in total")]
    TooManyModuleWeightedOps { limit: u64 },
    #[error("the module's operators times locals, summed over functions, is over {limit}")]
    OpsTimesLocalsTooLarge { limit: u64 },
    #[error("the module's typed blocks have more than {limit} parameters and results in total")]
    ModuleBlockArityTooLarge { limit: u64 },
    #[error("the module's element segments have more than {limit} entries in total")]
    TooManyElementEntries { limit: u64 },
    #[error("the module's tables start with more than {limit} entries in total")]
    TooManyTableEntries { limit: u64 },
    #[error("the module has more than {limit} data segments")]
    TooManyDataSegments { limit: u64 },
    #[error("the module's data segments hold more than {limit} bytes")]
    TooManyDataBytes { limit: u64 },
    #[error(
        "the module's operators times operand stack height, summed over functions, is over {limit}"
    )]
    OpsTimesStackTooLarge { limit: u64 },
    #[error(
        "the module's branch edges times nesting depth times locals, summed over functions, is over {limit}"
    )]
    EdgesTimesDepthTimesLocalsTooLarge { limit: u64 },
    #[error("the module has more than {limit} functions, imported and defined")]
    TooManyFunctionsTotal { limit: u64 },
    #[error("the module exports more than {limit} functions")]
    TooManyExportedFunctions { limit: u64 },
    #[error("the module's locals and parameters, summed over functions, are over {limit}")]
    TooManyModuleLocals { limit: u64 },
    #[error("the module has more than {limit} operators")]
    TooManyModuleOps { limit: u64 },
    #[error("the module's operators times frames, summed over functions, is over {limit}")]
    OpsTimesFramesTooLarge { limit: u64 },
    #[error("the module's locals times branch edges, summed over functions, is over {limit}")]
    LocalsTimesEdgesTooLarge { limit: u64 },
    #[error("the parameters and results of the module's functions sum to more than {limit}")]
    FunctionArityTooLarge { limit: u64 },
    #[error("the parameters and results of the module's types sum to more than {limit}")]
    TypeArityTooLarge { limit: u64 },
    /// Found by the bounded compile check, not by the scan.
    #[error("checking the module used more CPU time than allowed")]
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))] // only the Linux worker finds it
    OverCpu,
    #[error("checking the module used more memory than allowed")]
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))] // only the Linux worker finds it
    OverMemory,
}

/// Refuse input over [`MAX_MODULE_BYTES`] before any parsing or conversion.
pub(crate) fn check_input_size(code: &[u8]) -> Result<(), CompileRefusal> {
    if code.len() > MAX_MODULE_BYTES {
        return Err(CompileRefusal::ModuleTooLarge {
            size: code.len(),
            limit: MAX_MODULE_BYTES,
        });
    }
    Ok(())
}

/// Validate `code` and refuse it if it is over any limit.
pub(crate) fn check_module(code: &[u8]) -> Result<ModuleShape, CompileRefusal> {
    scan(code, true)
}

/// Validate `code` and report its shape without enforcing the limits (for the
/// corpus measurement). Malformed modules are still errors.
#[cfg(test)]
pub(crate) fn measure(code: &[u8]) -> Result<ModuleShape, CompileRefusal> {
    scan(code, false)
}

macro_rules! bump {
    ($enforce:expr, $slot:expr, $value:expr, $limit:expr, $err:expr) => {{
        let v = u64::from($value);
        if v > $slot {
            $slot = v;
        }
        if $enforce && v > u64::from($limit) {
            return Err($err);
        }
    }};
}

/// `(params, results)` of function type `idx`, if it is a function type.
fn func_type_arity(resources: &impl WasmModuleResources, idx: u32) -> Option<(u64, u64)> {
    let sub = resources.sub_type_at(idx)?;
    let CompositeInnerType::Func(f) = &sub.composite_type.inner else {
        return None;
    };
    Some((f.params().len() as u64, f.results().len() as u64))
}

/// Version of the operator weights, the cheap and common operator lists and the
/// block-splitting list below. It is part of the limits fingerprint, so a change
/// to any of them invalidates refusals remembered under the old model.
pub(crate) const OPERATOR_MODEL_VERSION: u32 = 3;

/// Weight of the lightest class of operators: constants, local and stack
/// manipulation, integer and float arithmetic, comparisons and conversions.
/// This is the unit.
pub(crate) const WEIGHT_CHEAP: u64 = 1;
/// Weight of the middle class of operators: loads, stores, calls, globals,
/// division, trapping float to integer conversion, and every SIMD operator.
pub(crate) const WEIGHT_COMMON: u64 = 96;
/// Weight of every other operator, including any operator this build does not
/// know. A new proposal's operators get this weight until they are measured.
pub(crate) const WEIGHT_HEAVY: u64 = 2_048;

macro_rules! define_vector_weights {
    (@weight simd $op:ident) => { Some(WEIGHT_COMMON) };
    (@weight relaxed_simd $op:ident) => { Some(WEIGHT_COMMON) };
    (@weight $other:ident $op:ident) => { None };
    ($( @$proposal:ident $op:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident ($($ann:tt)*) )*) => {
        /// The weight of a SIMD or relaxed-SIMD operator, `None` for any other.
        // `Operator` is non-exhaustive, so a wildcard arm is required.
        #[allow(clippy::wildcard_enum_match_arm)]
        fn vector_operator_weight(op: &wasmparser::Operator) -> Option<u64> {
            match op {
                $( wasmparser::Operator::$op { .. } => define_vector_weights!(@weight $proposal $op), )*
                _ => None,
            }
        }
    };
}
wasmparser::for_each_operator!(define_vector_weights);

/// Weight of `op`. Only the operators listed as cheap or common are
/// lighter than [`WEIGHT_HEAVY`]; an operator that is not named here, including
/// one added by a later `wasmparser`, is heavy.
// The wildcard arm is deliberate: it is the heavy default.
#[allow(clippy::wildcard_enum_match_arm)]
fn operator_weight(op: &wasmparser::Operator) -> u64 {
    use wasmparser::Operator as O;
    if let Some(w) = vector_operator_weight(op) {
        return w;
    }
    match op {
        // Structure, stack and locals (frames and branch edges are counted
        // separately).
        O::Unreachable
        | O::Nop
        | O::Block { .. }
        | O::Loop { .. }
        | O::If { .. }
        | O::Else
        | O::End
        | O::Br { .. }
        | O::BrIf { .. }
        | O::BrTable { .. }
        | O::Return
        | O::Drop
        | O::Select
        | O::TypedSelect { .. }
        | O::LocalGet { .. }
        | O::LocalSet { .. }
        | O::LocalTee { .. }
        | O::RefNull { .. }
        | O::RefIsNull
        // Constants.
        | O::I32Const { .. }
        | O::I64Const { .. }
        | O::F32Const { .. }
        | O::F64Const { .. }
        // Comparisons.
        | O::I32Eqz | O::I32Eq | O::I32Ne | O::I32LtS | O::I32LtU | O::I32GtS | O::I32GtU
        | O::I32LeS | O::I32LeU | O::I32GeS | O::I32GeU
        | O::I64Eqz | O::I64Eq | O::I64Ne | O::I64LtS | O::I64LtU | O::I64GtS | O::I64GtU
        | O::I64LeS | O::I64LeU | O::I64GeS | O::I64GeU
        | O::F32Eq | O::F32Ne | O::F32Lt | O::F32Gt | O::F32Le | O::F32Ge
        | O::F64Eq | O::F64Ne | O::F64Lt | O::F64Gt | O::F64Le | O::F64Ge
        // Integer arithmetic without division.
        | O::I32Clz | O::I32Ctz | O::I32Popcnt | O::I32Add | O::I32Sub | O::I32Mul
        | O::I32And | O::I32Or | O::I32Xor | O::I32Shl | O::I32ShrS | O::I32ShrU
        | O::I32Rotl | O::I32Rotr
        | O::I64Clz | O::I64Ctz | O::I64Popcnt | O::I64Add | O::I64Sub | O::I64Mul
        | O::I64And | O::I64Or | O::I64Xor | O::I64Shl | O::I64ShrS | O::I64ShrU
        | O::I64Rotl | O::I64Rotr
        // Float arithmetic.
        | O::F32Abs | O::F32Neg | O::F32Ceil | O::F32Floor | O::F32Trunc | O::F32Nearest
        | O::F32Sqrt | O::F32Add | O::F32Sub | O::F32Mul | O::F32Div | O::F32Min | O::F32Max
        | O::F32Copysign
        | O::F64Abs | O::F64Neg | O::F64Ceil | O::F64Floor | O::F64Trunc | O::F64Nearest
        | O::F64Sqrt | O::F64Add | O::F64Sub | O::F64Mul | O::F64Div | O::F64Min | O::F64Max
        | O::F64Copysign
        // Conversions that cannot trap.
        | O::I32WrapI64 | O::I64ExtendI32S | O::I64ExtendI32U
        | O::F32ConvertI32S | O::F32ConvertI32U | O::F32ConvertI64S | O::F32ConvertI64U
        | O::F32DemoteF64
        | O::F64ConvertI32S | O::F64ConvertI32U | O::F64ConvertI64S | O::F64ConvertI64U
        | O::F64PromoteF32
        | O::I32ReinterpretF32 | O::I64ReinterpretF64 | O::F32ReinterpretI32 | O::F64ReinterpretI64
        | O::I32Extend8S | O::I32Extend16S | O::I64Extend8S | O::I64Extend16S | O::I64Extend32S
        | O::I32TruncSatF32S | O::I32TruncSatF32U | O::I32TruncSatF64S | O::I32TruncSatF64U
        | O::I64TruncSatF32S | O::I64TruncSatF32U | O::I64TruncSatF64S | O::I64TruncSatF64U => {
            WEIGHT_CHEAP
        }
        // Common and moderately costly.
        O::Call { .. }
        | O::GlobalGet { .. }
        | O::GlobalSet { .. }
        | O::MemorySize { .. }
        | O::TableSize { .. }
        | O::DataDrop { .. }
        | O::ElemDrop { .. }
        | O::RefFunc { .. }
        | O::I32DivS | O::I32DivU | O::I32RemS | O::I32RemU
        | O::I64DivS | O::I64DivU | O::I64RemS | O::I64RemU
        | O::I32TruncF32S | O::I32TruncF32U | O::I32TruncF64S | O::I32TruncF64U
        | O::I64TruncF32S | O::I64TruncF32U | O::I64TruncF64S | O::I64TruncF64U
        | O::I32Load { .. } | O::I64Load { .. } | O::F32Load { .. } | O::F64Load { .. }
        | O::I32Load8S { .. } | O::I32Load8U { .. } | O::I32Load16S { .. } | O::I32Load16U { .. }
        | O::I64Load8S { .. } | O::I64Load8U { .. } | O::I64Load16S { .. } | O::I64Load16U { .. }
        | O::I64Load32S { .. } | O::I64Load32U { .. }
        | O::I32Store { .. } | O::I64Store { .. } | O::F32Store { .. } | O::F64Store { .. }
        | O::I32Store8 { .. } | O::I32Store16 { .. }
        | O::I64Store8 { .. } | O::I64Store16 { .. } | O::I64Store32 { .. } => WEIGHT_COMMON,
        // Everything else, known or not.
        _ => WEIGHT_HEAVY,
    }
}

/// Operators that split the function into more blocks. Each is counted as a
/// control frame.
// The wildcard arm is deliberate: only these operators split blocks.
#[allow(clippy::wildcard_enum_match_arm)]
fn creates_blocks(op: &wasmparser::Operator) -> bool {
    use wasmparser::Operator as O;
    matches!(
        op,
        O::TableGet { .. }
            | O::TableSet { .. }
            | O::TableGrow { .. }
            | O::TableFill { .. }
            | O::TableCopy { .. }
            | O::TableInit { .. }
            | O::CallIndirect { .. }
            | O::ReturnCallIndirect { .. }
            | O::CallRef { .. }
            | O::ReturnCallRef { .. }
            | O::RefAsNonNull
            | O::RefCastNonNull { .. }
            | O::RefCastNullable { .. }
    )
}

fn scan(code: &[u8], enforce: bool) -> Result<ModuleShape, CompileRefusal> {
    scan_with(code, enforce, accepted_features())
}

/// [`scan`] with an explicit feature set (the tests use it to count operators
/// the accepted set refuses).
fn scan_with(
    code: &[u8],
    enforce: bool,
    features: WasmFeatures,
) -> Result<ModuleShape, CompileRefusal> {
    check_input_size(code)?;
    if code.len() < 4 || &code[..4] != b"\0asm" {
        return Err(CompileRefusal::NotBinary("missing magic header".into()));
    }
    let malformed = |e: wasmparser::BinaryReaderError| CompileRefusal::Malformed(bounded_text(&e));
    let mut shape = ModuleShape {
        module_bytes: code.len() as u64,
        ..ModuleShape::default()
    };
    let mut validator = Validator::new_with_features(features);
    let mut allocs = FuncValidatorAllocations::default();
    let mut module_frames: u64 = 0;
    let mut module_locals_frames: u64 = 0;
    let mut module_frames_squared: u64 = 0;
    let mut module_edges_squared: u64 = 0;
    let mut module_ops: u64 = 0;
    let mut module_wops: u64 = 0;
    let mut module_locals: u64 = 0;
    let mut module_ops_stack: u64 = 0;
    let mut module_edges_depth_locals: u64 = 0;
    let mut module_functions_total: u64 = 0;
    let mut module_exported_functions: u64 = 0;
    let mut module_block_arity: u64 = 0;
    let mut module_elements: u64 = 0;
    let mut module_table_entries: u64 = 0;
    let mut module_data_segments: u64 = 0;
    let mut module_data_bytes: u64 = 0;
    let mut module_ops_locals: u64 = 0;
    let mut module_ops_frames: u64 = 0;
    let mut module_locals_edges: u64 = 0;
    let mut module_fn_arity: u64 = 0;
    let mut module_type_arity: u64 = 0;
    // `params + results` of each type, by type index (0 for a non-function type).
    let mut type_arities: Vec<u64> = Vec::new();

    for payload in Parser::new(0).parse_all(code) {
        let payload = payload.map_err(malformed)?;
        if let Payload::TypeSection(reader) = &payload {
            // Every member of a recursion group is a type.
            let mut types: u64 = 0;
            for group in reader.clone() {
                types = types.saturating_add(group.map_err(malformed)?.types().len() as u64);
                bump!(
                    enforce,
                    shape.type_entries,
                    types.min(u64::from(u32::MAX)),
                    MAX_TYPE_ENTRIES,
                    CompileRefusal::TooManyTypes {
                        limit: MAX_TYPE_ENTRIES
                    }
                );
            }
        } else if let Payload::FunctionSection(reader) = &payload {
            bump!(
                enforce,
                shape.functions,
                reader.count(),
                MAX_FUNCTIONS,
                CompileRefusal::TooManyFunctions {
                    limit: MAX_FUNCTIONS
                }
            );
        }
        // Function types are counted after the type section has been validated,
        // so the group structure is already known to be well formed.
        let valid = validator.payload(&payload).map_err(malformed)?;
        if let Payload::TypeSection(reader) = &payload {
            for group in reader.clone() {
                for ty in group.map_err(malformed)?.types() {
                    let arity = match &ty.composite_type.inner {
                        CompositeInnerType::Func(f) => {
                            (f.params().len() + f.results().len()) as u64
                        }
                        CompositeInnerType::Array(_)
                        | CompositeInnerType::Struct(_)
                        | CompositeInnerType::Cont(_) => 0,
                    };
                    type_arities.push(arity);
                    module_type_arity = module_type_arity.saturating_add(arity);
                    bump!(
                        enforce,
                        shape.type_arity_per_module,
                        module_type_arity,
                        MAX_MODULE_TYPE_ARITY,
                        CompileRefusal::TypeArityTooLarge {
                            limit: MAX_MODULE_TYPE_ARITY
                        }
                    );
                    if let CompositeInnerType::Func(f) = &ty.composite_type.inner {
                        let (p, r) = (f.params().len() as u64, f.results().len() as u64);
                        shape.fn_type_params = shape.fn_type_params.max(p);
                        shape.fn_type_results = shape.fn_type_results.max(r);
                        if enforce
                            && (p > u64::from(MAX_FN_TYPE_PARAMS)
                                || r > u64::from(MAX_FN_TYPE_RESULTS))
                        {
                            return Err(CompileRefusal::FunctionTypeTooLarge {
                                limit: MAX_FN_TYPE_PARAMS,
                                results: MAX_FN_TYPE_RESULTS,
                            });
                        }
                    }
                }
            }
        }
        #[allow(clippy::wildcard_enum_match_arm)]
        match &payload {
            Payload::ElementSection(reader) => {
                for element in reader.clone() {
                    let element = element.map_err(malformed)?;
                    let entries = match element.items {
                        wasmparser::ElementItems::Functions(r) => r.count(),
                        wasmparser::ElementItems::Expressions(_, r) => r.count(),
                    };
                    module_elements = module_elements.saturating_add(u64::from(entries));
                    bump!(
                        enforce,
                        shape.element_entries,
                        module_elements,
                        MAX_MODULE_ELEMENT_ENTRIES,
                        CompileRefusal::TooManyElementEntries {
                            limit: MAX_MODULE_ELEMENT_ENTRIES
                        }
                    );
                }
            }
            Payload::TableSection(reader) => {
                for table in reader.clone() {
                    let table = table.map_err(malformed)?;
                    module_table_entries = module_table_entries.saturating_add(table.ty.initial);
                    bump!(
                        enforce,
                        shape.table_entries,
                        module_table_entries,
                        MAX_MODULE_TABLE_ENTRIES,
                        CompileRefusal::TooManyTableEntries {
                            limit: MAX_MODULE_TABLE_ENTRIES
                        }
                    );
                }
            }
            Payload::DataSection(reader) => {
                for data in reader.clone() {
                    let data = data.map_err(malformed)?;
                    module_data_segments = module_data_segments.saturating_add(1);
                    module_data_bytes = module_data_bytes.saturating_add(data.data.len() as u64);
                    bump!(
                        enforce,
                        shape.data_segments,
                        module_data_segments,
                        MAX_MODULE_DATA_SEGMENTS,
                        CompileRefusal::TooManyDataSegments {
                            limit: MAX_MODULE_DATA_SEGMENTS
                        }
                    );
                    bump!(
                        enforce,
                        shape.data_bytes,
                        module_data_bytes,
                        MAX_MODULE_DATA_TOTAL,
                        CompileRefusal::TooManyDataBytes {
                            limit: MAX_MODULE_DATA_TOTAL
                        }
                    );
                }
            }
            _ => {}
        }
        // Every function, imported or defined, gets a call trampoline sized by
        // its type, so the parameters and results of all of them are summed.
        // Type indices are valid: the section was just validated.
        let mut fn_type_indices: Vec<u32> = Vec::new();
        #[allow(clippy::wildcard_enum_match_arm)]
        match &payload {
            Payload::FunctionSection(reader) => {
                for idx in reader.clone() {
                    fn_type_indices.push(idx.map_err(malformed)?);
                }
            }
            Payload::ImportSection(reader) => {
                for import in reader.clone().into_imports() {
                    if let wasmparser::TypeRef::Func(idx) | wasmparser::TypeRef::FuncExact(idx) =
                        import.map_err(malformed)?.ty
                    {
                        fn_type_indices.push(idx);
                    }
                }
            }
            _ => {}
        }
        module_functions_total =
            module_functions_total.saturating_add(fn_type_indices.len() as u64);
        bump!(
            enforce,
            shape.functions_total,
            module_functions_total,
            MAX_MODULE_FUNCTIONS_TOTAL,
            CompileRefusal::TooManyFunctionsTotal {
                limit: MAX_MODULE_FUNCTIONS_TOTAL
            }
        );
        if let Payload::ExportSection(reader) = &payload {
            for export in reader.clone() {
                if matches!(
                    export.map_err(malformed)?.kind,
                    wasmparser::ExternalKind::Func
                ) {
                    module_exported_functions = module_exported_functions.saturating_add(1);
                    bump!(
                        enforce,
                        shape.exported_functions,
                        module_exported_functions,
                        MAX_MODULE_EXPORTED_FUNCTIONS,
                        CompileRefusal::TooManyExportedFunctions {
                            limit: MAX_MODULE_EXPORTED_FUNCTIONS
                        }
                    );
                }
            }
        }
        for idx in fn_type_indices {
            let arity = type_arities.get(idx as usize).copied().unwrap_or(0);
            module_fn_arity = module_fn_arity.saturating_add(arity);
            bump!(
                enforce,
                shape.fn_arity_per_module,
                module_fn_arity,
                MAX_MODULE_FN_ARITY,
                CompileRefusal::FunctionArityTooLarge {
                    limit: MAX_MODULE_FN_ARITY
                }
            );
        }
        let ValidPayload::Func(to_validate, body) = valid else {
            continue;
        };

        let body_bytes = {
            let r = body.range();
            (r.end - r.start) as u64
        };
        bump!(
            enforce,
            shape.body_bytes_per_fn,
            body_bytes,
            MAX_FUNCTION_BODY_BYTES as u64,
            CompileRefusal::FunctionTooLarge {
                limit: MAX_FUNCTION_BODY_BYTES
            }
        );
        let mut fv = to_validate.into_validator(std::mem::take(&mut allocs));
        let mut reader = body.get_binary_reader();
        fv.read_locals(&mut reader).map_err(malformed)?;
        let locals = u64::from(fv.len_locals());
        bump!(
            enforce,
            shape.locals_plus_params_per_fn,
            locals,
            MAX_FUNCTION_LOCALS,
            CompileRefusal::TooManyLocals {
                limit: MAX_FUNCTION_LOCALS
            }
        );
        module_locals = module_locals.saturating_add(locals);
        bump!(
            enforce,
            shape.locals_per_module,
            module_locals,
            MAX_MODULE_LOCALS,
            CompileRefusal::TooManyModuleLocals {
                limit: MAX_MODULE_LOCALS
            }
        );
        bump!(
            enforce,
            shape.locals_times_body,
            locals.saturating_mul(body_bytes),
            MAX_FUNCTION_LOCALS_TIMES_BODY,
            CompileRefusal::LocalsTimesBodyTooLarge {
                limit: MAX_FUNCTION_LOCALS_TIMES_BODY
            }
        );

        let (mut frames, mut edges, mut block_arity): (u64, u64, u64) = (0, 0, 0);
        let mut ops: u64 = 0;
        let mut wops: u64 = 0;
        // The validator's exact operand stack height and control depth, the
        // tallest of each in this function.
        let (mut stack_max, mut depth_max): (u64, u64) = (0, 0);
        while !reader.eof() {
            let pos = reader.original_position();
            let op = reader.peek_operator(&fv.visitor(pos)).map_err(malformed)?;
            use wasmparser::Operator as O;
            // Every kind of control frame is counted, whether or not the
            // accepted feature set allows it: the counts do not rely on the
            // validator refusing an operator.
            // The wildcard arm is deliberate: only these operators open a frame.
            #[allow(clippy::wildcard_enum_match_arm)]
            let frame_type = match &op {
                O::Block { blockty }
                | O::Loop { blockty }
                | O::If { blockty }
                | O::Try { blockty } => Some(*blockty),
                O::TryTable { try_table } => Some(try_table.ty),
                _ => None,
            };
            let weight = operator_weight(&op);
            wops = wops.saturating_add(weight);
            module_wops = module_wops.saturating_add(weight);
            ops += 1;
            module_ops += 1;
            bump!(
                enforce,
                shape.weighted_ops_per_fn,
                wops,
                MAX_FUNCTION_WEIGHTED_OPS,
                CompileRefusal::TooManyWeightedOps {
                    limit: MAX_FUNCTION_WEIGHTED_OPS
                }
            );
            bump!(
                enforce,
                shape.weighted_ops_per_module,
                module_wops,
                MAX_MODULE_WEIGHTED_OPS,
                CompileRefusal::TooManyModuleWeightedOps {
                    limit: MAX_MODULE_WEIGHTED_OPS
                }
            );
            bump!(
                enforce,
                shape.ops_per_fn,
                ops,
                MAX_FUNCTION_OPS,
                CompileRefusal::TooManyOps {
                    limit: MAX_FUNCTION_OPS
                }
            );
            bump!(
                enforce,
                shape.ops_per_module,
                module_ops,
                MAX_MODULE_OPS,
                CompileRefusal::TooManyModuleOps {
                    limit: MAX_MODULE_OPS
                }
            );
            // A block-creating operator is a frame with no block type.
            let frame_type = frame_type.or_else(|| creates_blocks(&op).then_some(BlockType::Empty));
            if let Some(blockty) = frame_type {
                frames += 1;
                module_frames += 1;
                bump!(
                    enforce,
                    shape.control_frames_per_fn,
                    frames,
                    MAX_FUNCTION_CONTROL_FRAMES,
                    CompileRefusal::TooManyControlFrames {
                        limit: MAX_FUNCTION_CONTROL_FRAMES
                    }
                );
                bump!(
                    enforce,
                    shape.control_frames_per_module,
                    module_frames,
                    MAX_MODULE_CONTROL_FRAMES,
                    CompileRefusal::TooManyModuleControlFrames {
                        limit: MAX_MODULE_CONTROL_FRAMES
                    }
                );
                if let BlockType::FuncType(idx) = blockty {
                    let arity = func_type_arity(fv.resources(), idx)
                        .map(|(params, results)| params + results)
                        .unwrap_or(0);
                    block_arity += arity;
                    module_block_arity = module_block_arity.saturating_add(arity);
                    bump!(
                        enforce,
                        shape.block_type_arity_per_module,
                        module_block_arity,
                        MAX_MODULE_BLOCK_TYPE_ARITY,
                        CompileRefusal::ModuleBlockArityTooLarge {
                            limit: MAX_MODULE_BLOCK_TYPE_ARITY
                        }
                    );
                    bump!(
                        enforce,
                        shape.block_type_arity,
                        arity,
                        MAX_BLOCK_TYPE_ARITY,
                        CompileRefusal::BlockArityTooLarge {
                            limit: MAX_BLOCK_TYPE_ARITY
                        }
                    );
                    bump!(
                        enforce,
                        shape.block_type_arity_sum_per_fn,
                        block_arity,
                        MAX_BLOCK_TYPE_ARITY_SUM,
                        CompileRefusal::BlockArityTotalTooLarge {
                            limit: MAX_BLOCK_TYPE_ARITY_SUM
                        }
                    );
                }
            }
            if let O::BrTable { targets } = &op {
                let n = targets.len();
                bump!(
                    enforce,
                    shape.br_table_targets,
                    n,
                    MAX_BR_TABLE_TARGETS,
                    CompileRefusal::BrTableTooLarge {
                        limit: MAX_BR_TABLE_TARGETS
                    }
                );
            }
            // Edges into a join, one per branch target.
            // The wildcard arm is deliberate: only these operators branch.
            #[allow(clippy::wildcard_enum_match_arm)]
            let new_edges: u64 = match &op {
                O::Br { .. }
                | O::BrIf { .. }
                | O::If { .. }
                | O::BrOnNull { .. }
                | O::BrOnNonNull { .. }
                | O::BrOnCast { .. }
                | O::BrOnCastFail { .. } => 1,
                O::BrTable { targets } => u64::from(targets.len()) + 1,
                O::TryTable { try_table } => try_table.catches.len() as u64,
                _ => 0,
            };
            if new_edges > 0 {
                edges = edges.saturating_add(new_edges);
                bump!(
                    enforce,
                    shape.branch_edges_per_fn,
                    edges.min(u64::from(u32::MAX)),
                    MAX_FUNCTION_BRANCH_EDGES,
                    CompileRefusal::TooManyBranchEdges {
                        limit: MAX_FUNCTION_BRANCH_EDGES
                    }
                );
            }
            reader
                .visit_operator(&mut fv.visitor(pos))
                .map_err(malformed)?
                .map_err(malformed)?;
            stack_max = stack_max.max(u64::from(fv.operand_stack_height()));
            depth_max = depth_max.max(u64::from(fv.control_stack_height()));
            bump!(
                enforce,
                shape.nesting_depth,
                fv.control_stack_height(),
                MAX_NESTING_DEPTH,
                CompileRefusal::NestingTooDeep {
                    limit: MAX_NESTING_DEPTH
                }
            );
            bump!(
                enforce,
                shape.operand_stack_height,
                fv.operand_stack_height(),
                MAX_OPERAND_STACK_HEIGHT,
                CompileRefusal::OperandStackTooTall {
                    limit: MAX_OPERAND_STACK_HEIGHT
                }
            );
        }
        let locals_frames = locals.saturating_mul(frames);
        bump!(
            enforce,
            shape.locals_times_frames_per_fn,
            locals_frames,
            MAX_FUNCTION_LOCALS_TIMES_FRAMES,
            CompileRefusal::LocalsTimesFramesTooLarge {
                limit: MAX_FUNCTION_LOCALS_TIMES_FRAMES
            }
        );
        module_frames_squared = module_frames_squared.saturating_add(frames.saturating_mul(frames));
        bump!(
            enforce,
            shape.control_frames_squared_per_module,
            module_frames_squared,
            MAX_MODULE_CONTROL_FRAMES_SQUARED,
            CompileRefusal::ModuleControlFramesSquaredTooLarge {
                limit: MAX_MODULE_CONTROL_FRAMES_SQUARED
            }
        );
        module_locals_frames = module_locals_frames.saturating_add(locals_frames);
        bump!(
            enforce,
            shape.locals_times_frames_per_module,
            module_locals_frames,
            MAX_MODULE_LOCALS_TIMES_FRAMES,
            CompileRefusal::ModuleLocalsTimesFramesTooLarge {
                limit: MAX_MODULE_LOCALS_TIMES_FRAMES
            }
        );
        module_edges_squared = module_edges_squared.saturating_add(edges.saturating_mul(edges));
        bump!(
            enforce,
            shape.branch_edges_squared_per_module,
            module_edges_squared,
            MAX_MODULE_BRANCH_EDGES_SQUARED,
            CompileRefusal::ModuleBranchEdgesSquaredTooLarge {
                limit: MAX_MODULE_BRANCH_EDGES_SQUARED
            }
        );
        module_ops_frames = module_ops_frames.saturating_add(wops.saturating_mul(frames.max(1)));
        bump!(
            enforce,
            shape.ops_times_frames_per_module,
            module_ops_frames,
            MAX_MODULE_OPS_TIMES_FRAMES,
            CompileRefusal::OpsTimesFramesTooLarge {
                limit: MAX_MODULE_OPS_TIMES_FRAMES
            }
        );
        module_ops_stack = module_ops_stack.saturating_add(wops.saturating_mul(stack_max));
        bump!(
            enforce,
            shape.ops_times_stack_per_module,
            module_ops_stack,
            MAX_MODULE_OPS_TIMES_STACK,
            CompileRefusal::OpsTimesStackTooLarge {
                limit: MAX_MODULE_OPS_TIMES_STACK
            }
        );
        module_edges_depth_locals = module_edges_depth_locals
            .saturating_add(edges.saturating_mul(depth_max).saturating_mul(locals));
        bump!(
            enforce,
            shape.edges_times_depth_times_locals_per_module,
            module_edges_depth_locals,
            MAX_MODULE_EDGES_TIMES_DEPTH_TIMES_LOCALS,
            CompileRefusal::EdgesTimesDepthTimesLocalsTooLarge {
                limit: MAX_MODULE_EDGES_TIMES_DEPTH_TIMES_LOCALS
            }
        );
        module_ops_locals = module_ops_locals.saturating_add(wops.saturating_mul(locals));
        bump!(
            enforce,
            shape.ops_times_locals_per_module,
            module_ops_locals,
            MAX_MODULE_OPS_TIMES_LOCALS,
            CompileRefusal::OpsTimesLocalsTooLarge {
                limit: MAX_MODULE_OPS_TIMES_LOCALS
            }
        );
        module_locals_edges = module_locals_edges.saturating_add(locals.saturating_mul(edges));
        bump!(
            enforce,
            shape.locals_times_edges_per_module,
            module_locals_edges,
            MAX_MODULE_LOCALS_TIMES_EDGES,
            CompileRefusal::LocalsTimesEdgesTooLarge {
                limit: MAX_MODULE_LOCALS_TIMES_EDGES
            }
        );
        reader
            .finish_expression(&fv.visitor(reader.original_position()))
            .map_err(malformed)?;
        allocs = fv.into_allocations();
    }
    Ok(shape)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Binary form of `wat`.
    pub(crate) fn wasm(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("generated module is valid")
    }

    fn locals_decl(total_with_param: usize) -> String {
        if total_with_param > 1 {
            format!("(local {})", "i32 ".repeat(total_with_param - 1))
        } else {
            String::new()
        }
    }

    /// One function with `n` nested blocks.
    pub(crate) fn nested_blocks(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (func (export \"f\") {}{}))",
            "(block ".repeat(n),
            ")".repeat(n)
        ))
    }

    /// One function with `n` loops one after another.
    pub(crate) fn loops(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (func (export \"f\") (param i32) {}))",
            "(loop (br_if 0 (local.get 0)))".repeat(n)
        ))
    }

    /// One function with `n` `if` frames one after another.
    pub(crate) fn ifs(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (func (export \"f\") (param i32) {}))",
            "(if (local.get 0) (then))".repeat(n)
        ))
    }

    /// `functions` functions with `blocks` blocks each.
    pub(crate) fn blocks_in_functions(blocks: usize, functions: usize) -> Vec<u8> {
        let body = "(block)".repeat(blocks);
        let funcs: String = (0..functions).map(|_| format!("(func {body})")).collect();
        wasm(&format!("(module {funcs})"))
    }

    /// `functions` functions with `locals` locals and `blocks` blocks each.
    pub(crate) fn locals_and_blocks(locals: usize, blocks: usize, functions: usize) -> Vec<u8> {
        let l = if locals > 0 {
            format!("(local {})", "i32 ".repeat(locals))
        } else {
            String::new()
        };
        let body = "(block)".repeat(blocks);
        let funcs: String = (0..functions)
            .map(|_| format!("(func {l} {body})"))
            .collect();
        wasm(&format!("(module {funcs})"))
    }

    /// `functions` functions, each with `locals` locals and parameters and `edges`
    /// conditional branches out of one block.
    pub(crate) fn locals_and_branches(locals: usize, edges: usize, functions: usize) -> Vec<u8> {
        let body = format!("(block {})", "(br_if 0 (local.get 0))".repeat(edges));
        let funcs: String = (0..functions)
            .map(|_| format!("(func (param i32) {} {body})", locals_decl(locals.max(1))))
            .collect();
        wasm(&format!("(module {funcs})"))
    }

    /// One function with one `br_table` of `targets` targets (and a default).
    pub(crate) fn br_table(targets: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (func (export \"f\") (block (br_table {} (i32.const 0)))))",
            "0 ".repeat(targets + 1)
        ))
    }

    /// `tables` blocks in one function, each ending in a `br_table` of `targets`
    /// targets to the enclosing loop, with `locals` locals and parameters.
    pub(crate) fn br_tables_to_a_loop(targets: usize, tables: usize, locals: usize) -> Vec<u8> {
        let table = format!(
            "(block (br_table {} 0 (local.get 0)))",
            "1 ".repeat(targets)
        );
        wasm(&format!(
            "(module (func (export \"f\") (param i32) {} (loop {})))",
            locals_decl(locals.max(1)),
            table.repeat(tables)
        ))
    }

    /// `blocks` sequential blocks, each of a type with `arity` params and no results.
    pub(crate) fn blocks_of_type_arity(arity: usize, blocks: usize) -> Vec<u8> {
        let params = "i32 ".repeat(arity);
        let one = format!(
            "{}(block (type $t){})",
            "(i32.const 0)".repeat(arity),
            "(drop)".repeat(arity)
        );
        wasm(&format!(
            "(module (type $t (func (param {params}))) (func (export \"f\") {}))",
            one.repeat(blocks)
        ))
    }

    /// `blocks` sequential blocks, each of a type with `params` params and
    /// `results` results.
    pub(crate) fn blocks_of_params_and_results(
        params: usize,
        results: usize,
        blocks: usize,
    ) -> Vec<u8> {
        let one = format!(
            "{}(block (type $t) {}{}){}",
            "(i32.const 0)".repeat(params),
            "(drop)".repeat(params),
            "(i32.const 0)".repeat(results),
            "(drop)".repeat(results)
        );
        wasm(&format!(
            "(module (type $t (func (param {}) (result {}))) (func (export \"f\") {}))",
            "i32 ".repeat(params),
            "i32 ".repeat(results),
            one.repeat(blocks)
        ))
    }

    pub(crate) fn locals_and_body(params: usize, locals: usize, nops: usize) -> Vec<u8> {
        let p = if params > 0 {
            format!("(param {})", "i32 ".repeat(params))
        } else {
            String::new()
        };
        let l = if locals > 0 {
            format!("(local {})", "i32 ".repeat(locals))
        } else {
            String::new()
        };
        wasm(&format!(
            "(module (func (export \"f\") {p} {l} {}))",
            "(nop)".repeat(nops)
        ))
    }

    /// `functions` functions of `locals` locals and `nops` no-ops each.
    pub(crate) fn locals_and_body_in_functions(
        locals: usize,
        nops: usize,
        functions: usize,
    ) -> Vec<u8> {
        let l = if locals > 0 {
            format!("(local {})", "i32 ".repeat(locals))
        } else {
            String::new()
        };
        let funcs: String = (0..functions)
            .map(|_| format!("(func {l} {})", "(nop)".repeat(nops)))
            .collect();
        wasm(&format!("(module {funcs})"))
    }

    pub(crate) fn many_functions(count: usize) -> Vec<u8> {
        wasm(&format!("(module {})", "(func)".repeat(count)))
    }

    pub(crate) fn many_types(count: usize) -> Vec<u8> {
        wasm(&format!("(module {})", "(type (func))".repeat(count)))
    }

    pub(crate) fn fn_type(params: usize, results: usize) -> Vec<u8> {
        let consts = "(i32.const 0)".repeat(results);
        wasm(&format!(
            "(module (func (export \"f\") (param {}) (result {}) {consts}))",
            "i32 ".repeat(params),
            "i32 ".repeat(results)
        ))
    }

    pub(crate) fn tall_stack(height: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (func (export \"f\") {}{}))",
            "(i32.const 0)".repeat(height),
            "(drop)".repeat(height)
        ))
    }

    /// A valid module padded with a custom section so it is `total` bytes.
    pub(crate) fn padded_to(total: usize) -> Vec<u8> {
        let mut m = b"\0asm\x01\0\0\0".to_vec();
        let pad = total - m.len() - 1 - 5 - 1; // id, 5-byte length, 1-byte name length
        m.push(0);
        let payload = 1 + pad;
        // 5-byte padded LEB128 length.
        let mut len = payload as u32;
        for i in 0..5 {
            let mut b = (len & 0x7f) as u8;
            len >>= 7;
            if i < 4 {
                b |= 0x80;
            }
            m.push(b);
        }
        m.push(b'a');
        m.resize(total, 0);
        m
    }

    /// Text-format module of `n` loops in one function (not converted).
    pub(crate) fn loops_text(n: usize) -> String {
        format!(
            "(module (func (export \"f\") (param i32) {}))",
            "(loop (br_if 0 (local.get 0)))".repeat(n)
        )
    }

    /// One function with `n` `table.get` operators (each a block-creating one).
    pub(crate) fn table_gets(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (table 1 funcref) (func (export \"f\") {}))",
            "(drop (table.get 0 (i32.const 0)))".repeat(n)
        ))
    }

    /// One function with `n` `call_indirect` operators.
    pub(crate) fn call_indirects(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (type $t (func)) (table 1 funcref) (func (export \"f\") {}))",
            "(call_indirect (type $t) (i32.const 0))".repeat(n)
        ))
    }

    /// `functions` functions with `frames` blocks and `nops` no-ops each.
    pub(crate) fn frames_and_nops(frames: usize, nops: usize, functions: usize) -> Vec<u8> {
        let body = format!("{}{}", "(block)".repeat(frames), "(nop)".repeat(nops));
        let funcs: String = (0..functions).map(|_| format!("(func {body})")).collect();
        wasm(&format!("(module {funcs})"))
    }

    /// `functions` functions with `params` parameters each and nothing else.
    pub(crate) fn functions_with_params(params: usize, functions: usize) -> Vec<u8> {
        let one = format!("(func (param {}))", "i32 ".repeat(params));
        wasm(&format!("(module {})", one.repeat(functions)))
    }

    /// `types` function types of `params` parameters each, and no functions.
    pub(crate) fn types_with_params(params: usize, types: usize) -> Vec<u8> {
        let one = format!("(type (func (param {})))", "i32 ".repeat(params));
        wasm(&format!("(module {})", one.repeat(types)))
    }

    /// `functions` functions, each with `frames` empty blocks, `fills`
    /// `memory.fill` operators (heavy) and `locals` locals.
    pub(crate) fn fills(fills: usize, frames: usize, locals: usize, functions: usize) -> Vec<u8> {
        let l = if locals > 0 {
            format!("(local {})", "i32 ".repeat(locals))
        } else {
            String::new()
        };
        let body = format!(
            "{}{}",
            "(block)".repeat(frames),
            "(memory.fill (i32.const 0) (i32.const 0) (i32.const 0))".repeat(fills)
        );
        let funcs: String = (0..functions)
            .map(|_| format!("(func {l} {body})"))
            .collect();
        wasm(&format!("(module (memory 1) {funcs})"))
    }

    /// A module with a table of one entry and a passive element segment of `n`
    /// function indices.
    pub(crate) fn element_entries_of(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (table 1 funcref) (func $f) (elem func {}))",
            "$f ".repeat(n)
        ))
    }

    /// A module with `n` initial table entries.
    pub(crate) fn table_entries_of(n: usize) -> Vec<u8> {
        wasm(&format!("(module (table {n} funcref))"))
    }

    /// A module with `n` active data segments of one byte each.
    pub(crate) fn data_segments_of(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module (memory 1) {})",
            "(data (i32.const 0) \"x\")".repeat(n)
        ))
    }

    /// `functions` functions with 64 blocks of a 32-parameter type each.
    pub(crate) fn typed_blocks_in_functions(functions: usize) -> Vec<u8> {
        let params = "i32 ".repeat(32);
        let one = format!(
            "{}(block (type $t){})",
            "(i32.const 0)".repeat(32),
            "(drop)".repeat(32)
        );
        let funcs: String = (0..functions)
            .map(|_| format!("(func {})", one.repeat(64)))
            .collect();
        wasm(&format!(
            "(module (type $t (func (param {params}))) {funcs})"
        ))
    }

    /// `functions` functions, each with 120 distinct constants held on the operand
    /// stack, `loads` loads at that depth, and the constants dropped.
    pub(crate) fn deep_stack_loads(loads: usize, functions: usize) -> Vec<u8> {
        let held = "(i32.const 1)".repeat(120);
        let body = format!(
            "{held}{}{}",
            "(drop (i32.load (i32.const 0)))".repeat(loads),
            "(drop)".repeat(120)
        );
        let funcs: String = (0..functions).map(|_| format!("(func {body})")).collect();
        wasm(&format!("(module (memory 1) {funcs})"))
    }

    /// `functions` functions, each with `depth` nested loops holding `edges`
    /// conditional branches and `locals` locals and parameters.
    pub(crate) fn nested_loops_with_branches(
        depth: usize,
        edges: usize,
        locals: usize,
        functions: usize,
    ) -> Vec<u8> {
        let body = format!(
            "{}{}{}",
            "(loop ".repeat(depth),
            "(br_if 0 (local.get 0))".repeat(edges),
            ")".repeat(depth)
        );
        let funcs: String = (0..functions)
            .map(|_| format!("(func (param i32) {} {body})", locals_decl(locals.max(1))))
            .collect();
        wasm(&format!("(module {funcs})"))
    }

    /// A module importing `n` functions.
    pub(crate) fn imported_functions(n: usize) -> Vec<u8> {
        wasm(&format!(
            "(module {})",
            "(import \"m\" \"f\" (func))".repeat(n)
        ))
    }

    /// A module defining and exporting `n` functions.
    pub(crate) fn exported_functions_of(n: usize) -> Vec<u8> {
        let funcs: String = (0..n)
            .map(|i| format!("(func (export \"f{i}\"))"))
            .collect();
        wasm(&format!("(module {funcs})"))
    }

    /// One function of `n` operators of the given text, repeated.
    pub(crate) fn repeated_ops(unit: &str, n: usize) -> String {
        format!(
            "(module (memory 1) (func (export \"f\") (param v128) {}))",
            unit.repeat(n)
        )
    }

    pub(crate) type Counter = fn(&ModuleShape) -> u64;

    /// One limit: how to build a module with `n` units of the counted quantity,
    /// where the quantity is read, its limit, and the refusal one unit past it.
    pub(crate) struct Case {
        pub(crate) name: &'static str,
        pub(crate) build: Box<dyn Fn(usize) -> Vec<u8>>,
        pub(crate) counter: Counter,
        pub(crate) limit: u64,
        pub(crate) refusal: CompileRefusal,
        /// Set when a module AT this limit is refused by another limit first:
        /// the limit is then still enforced (the first module past it is refused
        /// with `refusal`), but it is not what bounds the module.
        pub(crate) shadowed_by: Option<CompileRefusal>,
    }

    /// The largest `n` for which the counted quantity of `build(n)` is at most
    /// `limit`; `build(n + 1)` is then over it. The quantity must be linear in
    /// `n`.
    pub(crate) fn solve(
        name: &str,
        build: &dyn Fn(usize) -> Vec<u8>,
        counter: Counter,
        limit: u64,
    ) -> usize {
        let value = |n: usize| {
            counter(
                &measure(&build(n))
                    .unwrap_or_else(|e| panic!("{name}: builder({n}) is not valid: {e}")),
            )
        };
        // Two points from n >= 1 give the line: some builders are not valid or
        // not linear at n = 0 (an empty function has no frame at all).
        let (one, two) = (value(1), value(2));
        assert!(
            two > one,
            "{name}: the counted quantity does not grow with n"
        );
        let slope = two - one;
        let zero = one - slope;
        assert!(limit >= one, "{name}: the limit {limit} is below one unit");
        let n = ((limit - zero) / slope) as usize;
        assert_eq!(
            value(n),
            zero + slope * n as u64,
            "{name}: builder is not linear in n"
        );
        assert!(
            value(n) <= limit && value(n + 1) > limit,
            "{name}: not at the limit"
        );
        n
    }

    impl Case {
        /// The `n` at which this case's counted quantity is exactly its limit.
        pub(crate) fn at_limit(&self) -> usize {
            solve(self.name, &*self.build, self.counter, self.limit)
        }
    }

    /// Largest divisor of `value` that is at most `max`.
    fn divisor(value: u64, max: u64) -> usize {
        (1..=max).rev().find(|d| value % d == 0).unwrap() as usize
    }

    macro_rules! case {
        ($name:expr, $build:expr, $field:ident, $limit:expr, $refusal:expr) => {
            Case {
                name: $name,
                build: Box::new($build),
                counter: |s| s.$field,
                limit: $limit as u64,
                refusal: $refusal,
                shadowed_by: None,
            }
        };
    }

    /// Every limit that a module can be built to hit exactly, one row per limit
    /// (the byte limit has its own test).
    pub(crate) fn limit_cases() -> Vec<Case> {
        // Per-function sizes at which a product limit is exactly met, used by the
        // per-module rows so one function is at ITS limit and the module sum is
        // what crosses.
        let frames_div = divisor(MAX_FUNCTION_LOCALS_TIMES_FRAMES, 400);
        let frames_per_fn_at = (MAX_FUNCTION_LOCALS_TIMES_FRAMES / frames_div as u64) as usize;
        let body_div = divisor(MAX_FUNCTION_LOCALS_TIMES_BODY, 400);
        // Edges per function in the per-module row: a power of two whose
        // square divides the module limit.
        let edges_each = 4_096usize;
        vec![
            case!(
                "nesting depth",
                nested_blocks,
                nesting_depth,
                MAX_NESTING_DEPTH,
                CompileRefusal::NestingTooDeep {
                    limit: MAX_NESTING_DEPTH
                }
            ),
            Case {
                shadowed_by: Some(CompileRefusal::ModuleControlFramesSquaredTooLarge {
                    limit: MAX_MODULE_CONTROL_FRAMES_SQUARED,
                }),
                ..case!(
                    "control frames per function (loops)",
                    loops,
                    control_frames_per_fn,
                    MAX_FUNCTION_CONTROL_FRAMES,
                    CompileRefusal::TooManyControlFrames {
                        limit: MAX_FUNCTION_CONTROL_FRAMES
                    }
                )
            },
            Case {
                shadowed_by: Some(CompileRefusal::ModuleControlFramesSquaredTooLarge {
                    limit: MAX_MODULE_CONTROL_FRAMES_SQUARED,
                }),
                ..case!(
                    "control frames per function (if frames)",
                    ifs,
                    control_frames_per_fn,
                    MAX_FUNCTION_CONTROL_FRAMES,
                    CompileRefusal::TooManyControlFrames {
                        limit: MAX_FUNCTION_CONTROL_FRAMES
                    }
                )
            },
            case!(
                "control frames per module",
                |n| blocks_in_functions(16, n),
                control_frames_per_module,
                MAX_MODULE_CONTROL_FRAMES,
                CompileRefusal::TooManyModuleControlFrames {
                    limit: MAX_MODULE_CONTROL_FRAMES
                }
            ),
            case!(
                "locals times control frames per function",
                move |n| locals_and_blocks(frames_div, n, 1),
                locals_times_frames_per_fn,
                MAX_FUNCTION_LOCALS_TIMES_FRAMES,
                CompileRefusal::LocalsTimesFramesTooLarge {
                    limit: MAX_FUNCTION_LOCALS_TIMES_FRAMES
                }
            ),
            case!(
                "locals times control frames per module",
                move |n| locals_and_blocks(frames_div, frames_per_fn_at, n),
                locals_times_frames_per_module,
                MAX_MODULE_LOCALS_TIMES_FRAMES,
                CompileRefusal::ModuleLocalsTimesFramesTooLarge {
                    limit: MAX_MODULE_LOCALS_TIMES_FRAMES
                }
            ),
            case!(
                "control frames squared per module",
                |n| blocks_in_functions(1_000, n),
                control_frames_squared_per_module,
                MAX_MODULE_CONTROL_FRAMES_SQUARED,
                CompileRefusal::ModuleControlFramesSquaredTooLarge {
                    limit: MAX_MODULE_CONTROL_FRAMES_SQUARED
                }
            ),
            case!(
                "branch edges per function",
                |n| locals_and_branches(1, n, 1),
                branch_edges_per_fn,
                MAX_FUNCTION_BRANCH_EDGES,
                CompileRefusal::TooManyBranchEdges {
                    limit: MAX_FUNCTION_BRANCH_EDGES
                }
            ),
            case!(
                "branch edges squared per module",
                move |n| locals_and_branches(1, edges_each, n),
                branch_edges_squared_per_module,
                MAX_MODULE_BRANCH_EDGES_SQUARED,
                CompileRefusal::ModuleBranchEdgesSquaredTooLarge {
                    limit: MAX_MODULE_BRANCH_EDGES_SQUARED
                }
            ),
            case!(
                "br_table targets",
                br_table,
                br_table_targets,
                MAX_BR_TABLE_TARGETS,
                CompileRefusal::BrTableTooLarge {
                    limit: MAX_BR_TABLE_TARGETS
                }
            ),
            case!(
                "block type arity",
                |n| blocks_of_type_arity(n, 1),
                block_type_arity,
                MAX_BLOCK_TYPE_ARITY,
                CompileRefusal::BlockArityTooLarge {
                    limit: MAX_BLOCK_TYPE_ARITY
                }
            ),
            case!(
                "block type arity (params plus results)",
                |n| blocks_of_params_and_results(n, MAX_FN_TYPE_RESULTS as usize, 1),
                block_type_arity,
                MAX_BLOCK_TYPE_ARITY,
                CompileRefusal::BlockArityTooLarge {
                    limit: MAX_BLOCK_TYPE_ARITY
                }
            ),
            case!(
                "block type arity sum",
                |n| blocks_of_type_arity(MAX_BLOCK_TYPE_ARITY as usize, n),
                block_type_arity_sum_per_fn,
                MAX_BLOCK_TYPE_ARITY_SUM,
                CompileRefusal::BlockArityTotalTooLarge {
                    limit: MAX_BLOCK_TYPE_ARITY_SUM
                }
            ),
            case!(
                "locals plus params",
                |n| locals_and_body(MAX_FN_TYPE_PARAMS as usize, n, 1),
                locals_plus_params_per_fn,
                MAX_FUNCTION_LOCALS,
                CompileRefusal::TooManyLocals {
                    limit: MAX_FUNCTION_LOCALS
                }
            ),
            case!(
                "functions",
                many_functions,
                functions,
                MAX_FUNCTIONS,
                CompileRefusal::TooManyFunctions {
                    limit: MAX_FUNCTIONS
                }
            ),
            case!(
                "type entries",
                many_types,
                type_entries,
                MAX_TYPE_ENTRIES,
                CompileRefusal::TooManyTypes {
                    limit: MAX_TYPE_ENTRIES
                }
            ),
            case!(
                "function type params",
                |n| fn_type(n, 1),
                fn_type_params,
                MAX_FN_TYPE_PARAMS,
                CompileRefusal::FunctionTypeTooLarge {
                    limit: MAX_FN_TYPE_PARAMS,
                    results: MAX_FN_TYPE_RESULTS,
                }
            ),
            case!(
                "function type results",
                |n| fn_type(0, n),
                fn_type_results,
                MAX_FN_TYPE_RESULTS,
                CompileRefusal::FunctionTypeTooLarge {
                    limit: MAX_FN_TYPE_PARAMS,
                    results: MAX_FN_TYPE_RESULTS,
                }
            ),
            case!(
                "operand stack height",
                tall_stack,
                operand_stack_height,
                MAX_OPERAND_STACK_HEIGHT,
                CompileRefusal::OperandStackTooTall {
                    limit: MAX_OPERAND_STACK_HEIGHT
                }
            ),
            // A body of single-byte operators is refused by the operator cap
            // before its byte limit; the byte limit still refuses the first body
            // past it.
            Case {
                shadowed_by: Some(CompileRefusal::TooManyOps {
                    limit: MAX_FUNCTION_OPS,
                }),
                ..case!(
                    "function body bytes",
                    |n| locals_and_body(0, 0, n),
                    body_bytes_per_fn,
                    MAX_FUNCTION_BODY_BYTES,
                    CompileRefusal::FunctionTooLarge {
                        limit: MAX_FUNCTION_BODY_BYTES
                    }
                )
            },
            case!(
                "locals times body per function",
                move |n| locals_and_body(0, body_div, n),
                locals_times_body,
                MAX_FUNCTION_LOCALS_TIMES_BODY,
                CompileRefusal::LocalsTimesBodyTooLarge {
                    limit: MAX_FUNCTION_LOCALS_TIMES_BODY
                }
            ),
            case!(
                "operators per function",
                |n| locals_and_body(0, 0, n),
                ops_per_fn,
                MAX_FUNCTION_OPS,
                CompileRefusal::TooManyOps {
                    limit: MAX_FUNCTION_OPS
                }
            ),
            case!(
                "operators per module",
                // 1,000 operators per function (999 no-ops and the end).
                |n| locals_and_body_in_functions(0, 999, n),
                ops_per_module,
                MAX_MODULE_OPS,
                CompileRefusal::TooManyModuleOps {
                    limit: MAX_MODULE_OPS
                }
            ),
            case!(
                "weighted operators per function",
                // Each memory.fill weighs 3 + WEIGHT_HEAVY.
                |n| fills(n, 0, 0, 1),
                weighted_ops_per_fn,
                MAX_FUNCTION_WEIGHTED_OPS,
                CompileRefusal::TooManyWeightedOps {
                    limit: MAX_FUNCTION_WEIGHTED_OPS
                }
            ),
            case!(
                "weighted operators per module",
                // 400 heavy operators per function.
                |n| fills(400, 0, 0, n),
                weighted_ops_per_module,
                MAX_MODULE_WEIGHTED_OPS,
                CompileRefusal::TooManyModuleWeightedOps {
                    limit: MAX_MODULE_WEIGHTED_OPS
                }
            ),
            case!(
                "operators times frames per module",
                // 500 frames and 200 heavy operators per function: few enough
                // functions that the module's other totals stay inside their limits.
                |n| fills(200, 500, 0, n),
                ops_times_frames_per_module,
                MAX_MODULE_OPS_TIMES_FRAMES,
                CompileRefusal::OpsTimesFramesTooLarge {
                    limit: MAX_MODULE_OPS_TIMES_FRAMES
                }
            ),
            case!(
                "operators times locals per module",
                // 100 locals and 20 heavy operators per function.
                |n| fills(20, 0, 100, n),
                ops_times_locals_per_module,
                MAX_MODULE_OPS_TIMES_LOCALS,
                CompileRefusal::OpsTimesLocalsTooLarge {
                    limit: MAX_MODULE_OPS_TIMES_LOCALS
                }
            ),
            case!(
                "element entries",
                element_entries_of,
                element_entries,
                MAX_MODULE_ELEMENT_ENTRIES,
                CompileRefusal::TooManyElementEntries {
                    limit: MAX_MODULE_ELEMENT_ENTRIES
                }
            ),
            case!(
                "table entries",
                table_entries_of,
                table_entries,
                MAX_MODULE_TABLE_ENTRIES,
                CompileRefusal::TooManyTableEntries {
                    limit: MAX_MODULE_TABLE_ENTRIES
                }
            ),
            case!(
                "data segments",
                data_segments_of,
                data_segments,
                MAX_MODULE_DATA_SEGMENTS,
                CompileRefusal::TooManyDataSegments {
                    limit: MAX_MODULE_DATA_SEGMENTS
                }
            ),
            case!(
                "typed block parameters per module",
                typed_blocks_in_functions,
                block_type_arity_per_module,
                MAX_MODULE_BLOCK_TYPE_ARITY,
                CompileRefusal::ModuleBlockArityTooLarge {
                    limit: MAX_MODULE_BLOCK_TYPE_ARITY
                }
            ),
            case!(
                "operators times operand stack height per module",
                // 120 values live and 300 loads: about 31,000 weighted operators
                // at stack height 121 per function.
                |n| deep_stack_loads(300, n),
                ops_times_stack_per_module,
                MAX_MODULE_OPS_TIMES_STACK,
                CompileRefusal::OpsTimesStackTooLarge {
                    limit: MAX_MODULE_OPS_TIMES_STACK
                }
            ),
            case!(
                "branch edges times nesting depth times locals per module",
                // 100 nested loops, 1,000 edges and 10 locals per function.
                |n| nested_loops_with_branches(100, 1_000, 10, n),
                edges_times_depth_times_locals_per_module,
                MAX_MODULE_EDGES_TIMES_DEPTH_TIMES_LOCALS,
                CompileRefusal::EdgesTimesDepthTimesLocalsTooLarge {
                    limit: MAX_MODULE_EDGES_TIMES_DEPTH_TIMES_LOCALS
                }
            ),
            case!(
                "functions imported and defined",
                imported_functions,
                functions_total,
                MAX_MODULE_FUNCTIONS_TOTAL,
                CompileRefusal::TooManyFunctionsTotal {
                    limit: MAX_MODULE_FUNCTIONS_TOTAL
                }
            ),
            case!(
                "exported functions",
                exported_functions_of,
                exported_functions,
                MAX_MODULE_EXPORTED_FUNCTIONS,
                CompileRefusal::TooManyExportedFunctions {
                    limit: MAX_MODULE_EXPORTED_FUNCTIONS
                }
            ),
            case!(
                "locals per module",
                |n| locals_and_body_in_functions(181, 0, n),
                locals_per_module,
                MAX_MODULE_LOCALS,
                CompileRefusal::TooManyModuleLocals {
                    limit: MAX_MODULE_LOCALS
                }
            ),
            case!(
                "locals times branch edges per module",
                // 100 locals and 294 branch edges per function.
                |n| locals_and_branches(100, 294, n),
                locals_times_edges_per_module,
                MAX_MODULE_LOCALS_TIMES_EDGES,
                CompileRefusal::LocalsTimesEdgesTooLarge {
                    limit: MAX_MODULE_LOCALS_TIMES_EDGES
                }
            ),
            case!(
                "function arity per module",
                |n| functions_with_params(92, n),
                fn_arity_per_module,
                MAX_MODULE_FN_ARITY,
                CompileRefusal::FunctionArityTooLarge {
                    limit: MAX_MODULE_FN_ARITY
                }
            ),
            case!(
                "type arity per module",
                |n| types_with_params(14, n),
                type_arity_per_module,
                MAX_MODULE_TYPE_ARITY,
                CompileRefusal::TypeArityTooLarge {
                    limit: MAX_MODULE_TYPE_ARITY
                }
            ),
        ]
    }

    /// A module shape (derived from `measure`) with every counter, as a map from
    /// field name to value, for comparing against an independent count.
    pub(crate) fn as_map(shape: &ModuleShape) -> serde_json::Map<String, serde_json::Value> {
        serde_json::to_value(shape)
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    /// Every deployed module is at least this many times inside every limit.
    const REFERENCE_MARGIN: u64 = 4;
    /// Limits exempt from the full margin, by name, with the margin they keep.
    /// `module_bytes` equals the code-size cap enforced elsewhere
    /// (`native_api::MAX_WASM_CODE_SIZE`), so a larger value here is
    /// unreachable. The operator-count and weighted limits keep a smaller
    /// margin, so that the largest accepted module stays inside the memory
    /// budget of the compile worker; the deployed modules stay under half of
    /// each.
    const MARGIN_EXEMPT: &[(&str, u64)] = &[
        ("module_bytes", 2),
        ("ops_per_module", 3),
        ("weighted_ops_per_fn", 2),
        ("weighted_ops_per_module", 2),
        ("ops_times_frames_per_module", 2),
        ("ops_times_locals_per_module", 2),
        ("edges_times_depth_times_locals_per_module", 2),
        ("functions", 2),
        ("functions_total", 2),
    ];
    /// Limits that no module in the corpus exercises (the corpus value is 0).
    const UNEXERCISED_BY_CORPUS: &[&str] = &[
        "block_type_arity",
        "block_type_arity_per_module",
        "block_type_arity_sum_per_fn",
    ];

    /// Changing any one limit changes the digest that remembered refusals are
    /// keyed by.
    #[test]
    fn changing_any_limit_changes_the_limits_fingerprint() {
        let base = ModuleShape::limits();
        let fields = as_map(&base);
        let mut seen = std::collections::HashSet::from([fingerprint_of(&base, 7)]);
        for (name, value) in &fields {
            let mut changed = fields.clone();
            changed[name] = (value.as_u64().unwrap() + 1).into();
            let shape: ModuleShape =
                serde_json::from_value(serde_json::Value::Object(changed)).unwrap();
            assert!(
                seen.insert(fingerprint_of(&shape, 7)),
                "{name} is not in the digest"
            );
        }
        assert_eq!(seen.len(), fields.len() + 1);
        assert_eq!(limits_fingerprint(7), fingerprint_of(&base, 7));
    }

    /// A different engine identity (a wasmtime bump, a changed engine
    /// configuration) gives a different digest.
    #[test]
    fn changing_the_engine_identity_changes_the_limits_fingerprint() {
        assert_ne!(limits_fingerprint(1), limits_fingerprint(2));
        assert_eq!(limits_fingerprint(1), limits_fingerprint(1));
    }

    /// The early warning starts at half of a limit and names the closest counter.
    #[test]
    fn the_nearest_limit_starts_at_half_and_names_the_closest_counter() {
        let limits = ModuleShape::limits();
        assert_eq!(nearest_limit(&ModuleShape::default()), None);
        let below = ModuleShape {
            nesting_depth: limits.nesting_depth / 2 - 1,
            ..Default::default()
        };
        assert_eq!(nearest_limit(&below), None);
        let at = ModuleShape {
            nesting_depth: limits.nesting_depth / 2,
            functions: limits.functions * 9 / 10,
            ..Default::default()
        };
        assert_eq!(
            nearest_limit(&at),
            Some(("functions", limits.functions * 9 / 10, limits.functions))
        );
        let only = ModuleShape {
            nesting_depth: limits.nesting_depth / 2,
            ..Default::default()
        };
        assert_eq!(
            nearest_limit(&only),
            Some((
                "nesting_depth",
                limits.nesting_depth / 2,
                limits.nesting_depth
            ))
        );
    }

    /// Each limit accepts a module whose counted quantity is EXACTLY the limit
    /// and refuses the module one unit past it, with that limit's refusal.
    #[test]
    fn every_limit_accepts_the_limit_and_refuses_one_past_it() {
        // Every case is checked and every failure reported, so a change that
        // breaks several limits names them all.
        let mut failures = Vec::new();
        for case in limit_cases() {
            let at = case.at_limit();
            let name = case.name;
            match &case.shadowed_by {
                None => {
                    if let Err(e) = check_module(&(case.build)(at)) {
                        failures.push(format!("{name}: the limit itself is refused: {e}"));
                    }
                }
                Some(first) => {
                    if check_module(&(case.build)(at)).err().as_ref() != Some(first) {
                        failures.push(format!(
                            "{name}: a module at this limit is not refused by another limit first"
                        ));
                    }
                }
            }
            match check_module(&(case.build)(at + 1)) {
                Err(got) if got == case.refusal => {}
                Err(got) => failures.push(format!("{name} at n={}: refused with {got}", at + 1)),
                Ok(_) => failures.push(format!(
                    "{name}: one past the limit (n={}) was accepted",
                    at + 1
                )),
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// One function's frames are bounded by the squared sum before the
    /// per-function limit: the largest accepted function has
    /// `floor(sqrt(MAX_MODULE_CONTROL_FRAMES_SQUARED))` frames.
    #[test]
    fn one_function_is_bounded_by_the_squared_sum_before_its_own_limit() {
        let frames = |n: usize| measure(&loops(n)).unwrap().control_frames_per_fn as usize;
        // `loops(n)` has one frame more than n or the same: solve for the count.
        let per = frames(2) - frames(1);
        assert_eq!(per, 1);
        let offset = frames(1) - 1;
        let max = (MAX_MODULE_CONTROL_FRAMES_SQUARED as f64).sqrt() as usize;
        let accepted = max - offset;
        assert!(check_module(&loops(accepted)).is_ok());
        assert_eq!(
            check_module(&loops(accepted + 1)),
            Err(CompileRefusal::ModuleControlFramesSquaredTooLarge {
                limit: MAX_MODULE_CONTROL_FRAMES_SQUARED
            })
        );
        assert!(max < MAX_FUNCTION_CONTROL_FRAMES as usize);
    }

    /// Frames and edges of the exception proposals are counted even though the
    /// accepted feature set refuses them as invalid: the counts must not rely on
    /// the validator refusing the operator.
    #[test]
    fn exception_frames_and_catch_edges_are_counted() {
        let try_tables = |n: usize, catches: &str| {
            wasm(&format!(
                "(module (tag $e) (func (export \"f\") {}))",
                format!("(block (try_table {catches} (nop)))").repeat(n)
            ))
        };
        // With every feature on the module validates, and each try_table is a
        // frame; the counts do not depend on enforcement.
        let m = try_tables(MAX_FUNCTION_CONTROL_FRAMES as usize / 2 + 1, "");
        let counted = scan_with(&m, false, WasmFeatures::all()).unwrap();
        assert!(
            counted.control_frames_per_fn > u64::from(MAX_FUNCTION_CONTROL_FRAMES),
            "block + try_table frames must be counted"
        );
        assert!(matches!(
            scan_with(&m, true, WasmFeatures::all()),
            Err(CompileRefusal::TooManyWeightedOps { .. }
                | CompileRefusal::TooManyControlFrames { .. })
        ));
        let shape = scan_with(&try_tables(10, ""), false, WasmFeatures::all()).unwrap();
        assert_eq!(shape.control_frames_per_fn, 20);
        // Each catch clause is an edge.
        let catches = scan_with(
            &try_tables(10, "(catch_all 0) (catch_all 0) (catch_all 0)"),
            false,
            WasmFeatures::all(),
        )
        .unwrap();
        assert_eq!(catches.branch_edges_per_fn, 30);
        // And the accepted feature set refuses the same module as invalid.
        assert!(matches!(
            check_module(&try_tables(1, "")),
            Err(CompileRefusal::Malformed(_))
        ));
    }

    /// Legacy `try` frames and reference branches are counted too, for the same
    /// reason: the accepted feature set refuses both.
    #[test]
    fn legacy_try_frames_and_reference_branches_are_counted() {
        // The text format no longer has legacy `try`, so the module is written
        // out: ten `try nop catch_all end` in one function.
        let body: Vec<u8> = std::iter::once(0u8) // no locals
            .chain([0x06, 0x40, 0x01, 0x19, 0x0b].repeat(10))
            .chain([0x0b])
            .collect();
        let mut legacy = b"\0asm\x01\0\0\0".to_vec();
        legacy.extend([0x01, 0x04, 0x01, 0x60, 0x00, 0x00]); // type () -> ()
        legacy.extend([0x03, 0x02, 0x01, 0x00]); // one function of that type
        legacy.extend([0x0a, body.len() as u8 + 2, 0x01, body.len() as u8]);
        legacy.extend(&body);
        let shape = scan_with(&legacy, false, WasmFeatures::all()).unwrap();
        assert_eq!(shape.control_frames_per_fn, 10);
        let branches = wasm(&format!(
            "(module (func (export \"f\") (param $r (ref null func)) {}))",
            "(block (br_on_null 0 (local.get $r)) (drop))".repeat(10)
        ));
        let shape = scan_with(&branches, false, WasmFeatures::all()).unwrap();
        assert_eq!(shape.branch_edges_per_fn, 10);
        assert_eq!(shape.control_frames_per_fn, 10);
        // Both are invalid for the accepted feature set.
        assert!(check_module(&legacy).is_err());
        assert!(check_module(&branches).is_err());
    }

    /// Every proposal outside the accepted set is refused as invalid.
    #[test]
    fn modules_using_other_proposals_are_refused_as_invalid() {
        let cases: &[(&str, &str)] = &[
            (
                "tail calls",
                "(module (func $g) (func (export \"f\") (return_call $g)))",
            ),
            (
                "exceptions",
                "(module (func (export \"f\") (try_table (nop))))",
            ),
            (
                "GC",
                "(module (type $s (struct (field i32))) (func (export \"f\") (drop (struct.new_default $s))))",
            ),
            (
                "function references",
                "(module (type $t (func)) (func $g) (elem declare func $g) (func (export \"f\") (call_ref $t (ref.func $g))))",
            ),
            (
                "threads",
                "(module (memory 1 1 shared) (func (export \"f\") (drop (i32.atomic.load (i32.const 0)))))",
            ),
            ("64-bit memory", "(module (memory i64 1))"),
            ("multiple memories", "(module (memory 1) (memory 1))"),
            (
                "relaxed SIMD",
                "(module (func (export \"f\") (param v128) (result v128) (i8x16.relaxed_swizzle (local.get 0) (local.get 0))))",
            ),
            (
                "wide arithmetic",
                "(module (func (export \"f\") (param i64 i64) (result i64 i64) (i64.add128 (local.get 0) (local.get 1) (local.get 0) (local.get 1))))",
            ),
            (
                "extended constants",
                "(module (global i32 (i32.add (i32.const 1) (i32.const 2))))",
            ),
        ];
        for (name, text) in cases {
            let binary = wat::parse_str(text)
                .unwrap_or_else(|e| panic!("{name}: the test module is not valid WAT: {e}"));
            // The module is valid with every proposal on, so the refusal below
            // is the proposal being off and nothing else.
            scan_with(&binary, true, WasmFeatures::all()).unwrap_or_else(|e| {
                panic!("{name}: the test module is invalid for another reason: {e}")
            });
            match check_module(&binary) {
                Err(CompileRefusal::Malformed(_)) => {}
                other => panic!("{name}: expected refusal as invalid, got {other:?}"),
            }
        }
    }

    /// The accepted feature set still takes a module that uses the WebAssembly
    /// 2.0 features compilers emit.
    #[test]
    fn modules_using_the_accepted_features_are_accepted() {
        let m = wasm(
            "(module (memory 1) (func (export \"f\") (param i32 i32) (result i32 i32)
                (memory.fill (i32.const 0) (i32.const 0) (i32.const 4))
                (i32.extend8_s (local.get 0))
                (i32.trunc_sat_f32_s (f32.const 1))
                (i32.add)
                (local.get 1)))",
        );
        check_module(&m).expect("2.0 features are accepted");
    }

    /// Counts are on RECURSION GROUP MEMBERS, not groups.
    #[test]
    fn every_type_in_a_recursion_group_is_counted() {
        let one_group = wasm(&format!(
            "(module (rec {}))",
            "(type (func))".repeat(MAX_TYPE_ENTRIES as usize + 1)
        ));
        // A multi-type group needs the GC proposal, so the accepted feature set
        // refuses it as invalid; with the proposal on, the count refuses it first.
        let err = scan_with(&one_group, true, WasmFeatures::all()).unwrap_err();
        assert_eq!(
            err,
            CompileRefusal::TooManyTypes {
                limit: MAX_TYPE_ENTRIES
            }
        );
        let shape = scan_with(
            &wasm("(module (rec (type (func)) (type (func))) (type (func)))"),
            false,
            WasmFeatures::all(),
        )
        .unwrap();
        assert_eq!(shape.type_entries, 3);
    }

    /// The shapes measured over deployed modules (`reference_shapes.json`,
    /// produced by the `corpus_shapes` test) stay inside every limit with the
    /// recorded margin, and every limit field is present in the table.
    #[test]
    fn reference_module_shapes_stay_inside_every_limit() {
        #[derive(Deserialize)]
        struct Entry {
            name: String,
            shape: serde_json::Value,
        }
        let entries: Vec<Entry> = serde_json::from_str(include_str!("reference_shapes.json"))
            .expect("reference_shapes.json parses");
        assert!(entries.len() >= 10, "the reference table is nearly empty");
        let limits = as_map(&ModuleShape::limits());
        let mut closest = std::collections::BTreeMap::<String, (u64, String)>::new();
        for entry in &entries {
            let shape = entry.shape.as_object().expect("shape is an object");
            assert_eq!(
                shape.len(),
                limits.len(),
                "{}: the recorded fields differ from ModuleShape (re-measure the corpus)",
                entry.name
            );
            for (field, limit) in &limits {
                let value = shape
                    .get(field)
                    .unwrap_or_else(|| panic!("{}: no `{field}` recorded", entry.name))
                    .as_u64()
                    .unwrap();
                let limit = limit.as_u64().unwrap();
                let margin = MARGIN_EXEMPT
                    .iter()
                    .find(|(name, _)| name == field)
                    .map_or(REFERENCE_MARGIN, |(_, m)| *m);
                assert!(
                    value.saturating_mul(margin) <= limit,
                    "{}: {field} is {value}, closer than {margin}x to the limit {limit}",
                    entry.name
                );
                let best = closest.entry(field.clone()).or_default();
                if value >= best.0 {
                    *best = (value, entry.name.clone());
                }
            }
        }
        // A limit nothing in the table comes near is not evidence for its value:
        // the fields the corpus never exercises are exactly these, and they
        // are listed so a change to the list is a decision.
        let unexercised: Vec<&str> = limits
            .keys()
            .filter(|f| closest[*f].0 == 0)
            .map(String::as_str)
            .collect();
        assert_eq!(unexercised, UNEXERCISED_BY_CORPUS, "{closest:?}");
    }

    /// An independent count (no validator, no shared helpers) of the quantities
    /// `scan` reports, so a drift in the scan's own counting cannot hide.
    fn independent_shape(code: &[u8]) -> serde_json::Map<String, serde_json::Value> {
        use wasmparser::{Operator as O, Parser, Payload};
        let mut fn_types: Vec<u32> = Vec::new();
        let mut import_fn_types: Vec<u32> = Vec::new();
        // Parameters, and parameters plus results, of each type.
        let mut type_params: Vec<u64> = Vec::new();
        let mut type_arity: Vec<u64> = Vec::new();
        let mut bodies = Vec::new();
        let (mut element_entries, mut table_entries) = (0u64, 0u64);
        let (mut data_segments, mut data_bytes) = (0u64, 0u64);
        let mut exported = 0u64;
        for payload in Parser::new(0).parse_all(code) {
            #[allow(clippy::wildcard_enum_match_arm)]
            match payload.unwrap() {
                Payload::TypeSection(r) => {
                    for group in r {
                        for ty in group.unwrap().types() {
                            if let wasmparser::CompositeInnerType::Func(f) =
                                &ty.composite_type.inner
                            {
                                type_params.push(f.params().len() as u64);
                                type_arity.push((f.params().len() + f.results().len()) as u64);
                            } else {
                                type_params.push(0);
                                type_arity.push(0);
                            }
                        }
                    }
                }
                Payload::ImportSection(r) => {
                    for import in r.into_imports() {
                        if let wasmparser::TypeRef::Func(t) = import.unwrap().ty {
                            import_fn_types.push(t);
                        }
                    }
                }
                Payload::FunctionSection(r) => {
                    fn_types = r.into_iter().map(|t| t.unwrap()).collect();
                }
                Payload::ElementSection(r) => {
                    for e in r {
                        element_entries += match e.unwrap().items {
                            wasmparser::ElementItems::Functions(f) => u64::from(f.count()),
                            wasmparser::ElementItems::Expressions(_, x) => u64::from(x.count()),
                        };
                    }
                }
                Payload::TableSection(r) => {
                    for t in r {
                        table_entries += t.unwrap().ty.initial;
                    }
                }
                Payload::ExportSection(r) => {
                    for e in r {
                        if matches!(e.unwrap().kind, wasmparser::ExternalKind::Func) {
                            exported += 1;
                        }
                    }
                }
                Payload::DataSection(r) => {
                    for d in r {
                        data_segments += 1;
                        data_bytes += d.unwrap().data.len() as u64;
                    }
                }
                Payload::CodeSectionEntry(body) => bodies.push(body),
                _ => {}
            }
        }
        let (mut frames_module, mut frames_sq) = (0u64, 0u64);
        let (mut max_body, mut max_locals, mut max_frames, mut max_edges) =
            (0u64, 0u64, 0u64, 0u64);
        let (mut max_lf, mut sum_lf, mut sum_e2) = (0u64, 0u64, 0u64);
        let (mut max_lb, mut max_brt, mut max_depth) = (0u64, 0u64, 0u64);
        let (mut max_ops, mut sum_ops, mut sum_of, mut sum_ol) = (0u64, 0u64, 0u64, 0u64);
        let (mut max_wops, mut sum_wops, mut module_block_arity) = (0u64, 0u64, 0u64);
        let mut sum_edl = 0u64;
        let (mut sum_locals, mut sum_le) = (0u64, 0u64);
        for (index, body) in bodies.iter().enumerate() {
            let bytes = body.range().end - body.range().start;
            let mut locals = type_params[fn_types[index] as usize];
            for l in body.get_locals_reader().unwrap() {
                locals += u64::from(l.unwrap().0);
            }
            let (mut frames, mut edges, mut depth, mut ops, mut wops) =
                (0u64, 0u64, 1u64, 0u64, 0u64);
            let mut max_depth_fn = 1u64;
            for op in body.get_operators_reader().unwrap() {
                let op = op.unwrap();
                // Weights, assigned from the operator's name (a second copy of
                // the model, so a drift in the scan's own lists shows here).
                let name = format!("{op:?}");
                let variant = name
                    .split(|c: char| !c.is_ascii_alphanumeric())
                    .next()
                    .unwrap()
                    .to_string();
                let vector = ["V128", "I8x16", "I16x8", "I32x4", "I64x2", "F32x4", "F64x2"]
                    .iter()
                    .any(|p| variant.starts_with(p));
                let cheap_set: &[&str] = &[
                    "Unreachable",
                    "Nop",
                    "Block",
                    "Loop",
                    "If",
                    "Else",
                    "End",
                    "Br",
                    "BrIf",
                    "BrTable",
                    "Return",
                    "Drop",
                    "Select",
                    "TypedSelect",
                    "LocalGet",
                    "LocalSet",
                    "LocalTee",
                    "RefNull",
                    "RefIsNull",
                    "I32Const",
                    "I64Const",
                    "F32Const",
                    "F64Const",
                ];
                let weight = if vector {
                    96
                } else if cheap_set.contains(&variant.as_str())
                    || (["I32", "I64", "F32", "F64"]
                        .iter()
                        .any(|p| variant.starts_with(p))
                        && !variant.contains("Div")
                        && !variant.contains("Rem")
                        && !variant.contains("Load")
                        && !variant.contains("Store")
                        && !(variant.contains("Trunc")
                            && !variant.contains("Sat")
                            && !["F32Trunc", "F64Trunc"].contains(&variant.as_str()))
                        && !variant.contains("Atomic")
                        && !variant.contains("Wait")
                        && !variant.contains("Cmpxchg"))
                {
                    1
                } else if ((variant.starts_with("I32")
                    || variant.starts_with("I64")
                    || variant.starts_with("F32")
                    || variant.starts_with("F64"))
                    && !variant.contains("Atomic"))
                    || [
                        "Call",
                        "GlobalGet",
                        "GlobalSet",
                        "MemorySize",
                        "TableSize",
                        "DataDrop",
                        "ElemDrop",
                        "RefFunc",
                    ]
                    .contains(&variant.as_str())
                {
                    96
                } else {
                    2048
                };
                wops += weight;
                ops += 1;
                if let O::Block { blockty } | O::Loop { blockty } | O::If { blockty } = &op {
                    if let wasmparser::BlockType::FuncType(t) = blockty {
                        module_block_arity += type_arity[*t as usize];
                    }
                }
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    O::Block { .. } | O::Loop { .. } => {
                        frames += 1;
                        depth += 1;
                    }
                    O::If { .. } => {
                        frames += 1;
                        edges += 1;
                        depth += 1;
                    }
                    O::TableGet { .. }
                    | O::TableSet { .. }
                    | O::TableGrow { .. }
                    | O::TableFill { .. }
                    | O::TableCopy { .. }
                    | O::TableInit { .. }
                    | O::CallIndirect { .. } => frames += 1,
                    O::End => depth -= 1,
                    O::Br { .. } | O::BrIf { .. } => edges += 1,
                    O::BrTable { targets } => {
                        edges += u64::from(targets.len()) + 1;
                        max_brt = max_brt.max(u64::from(targets.len()));
                    }
                    _ => {}
                }
                max_depth = max_depth.max(depth);
                max_depth_fn = max_depth_fn.max(depth);
            }
            sum_edl += edges * max_depth_fn * locals;
            max_body = max_body.max(bytes as u64);
            max_locals = max_locals.max(locals);
            max_frames = max_frames.max(frames);
            max_edges = max_edges.max(edges);
            frames_module += frames;
            frames_sq += frames * frames;
            max_lf = max_lf.max(locals * frames);
            sum_lf += locals * frames;
            sum_e2 += edges * edges;
            max_lb = max_lb.max(locals * bytes as u64);
            max_ops = max_ops.max(ops);
            sum_ops += ops;
            max_wops = max_wops.max(wops);
            sum_wops += wops;
            sum_of += wops * frames.max(1);
            sum_ol += wops * locals;
            sum_locals += locals;
            sum_le += locals * edges;
        }
        let fn_arity: u64 = fn_types
            .iter()
            .chain(&import_fn_types)
            .map(|t| type_arity[*t as usize])
            .sum();
        let mut m = serde_json::Map::new();
        for (k, v) in [
            ("module_bytes", code.len() as u64),
            ("functions", bodies.len() as u64),
            ("body_bytes_per_fn", max_body),
            ("control_frames_per_fn", max_frames),
            ("control_frames_per_module", frames_module),
            ("control_frames_squared_per_module", frames_sq),
            ("branch_edges_per_fn", max_edges),
            ("br_table_targets", max_brt),
            ("nesting_depth", max_depth),
            ("locals_plus_params_per_fn", max_locals),
            ("locals_times_frames_per_fn", max_lf),
            ("locals_times_frames_per_module", sum_lf),
            ("branch_edges_squared_per_module", sum_e2),
            ("locals_times_body", max_lb),
            ("ops_per_fn", max_ops),
            ("ops_per_module", sum_ops),
            ("weighted_ops_per_fn", max_wops),
            ("weighted_ops_per_module", sum_wops),
            ("block_type_arity_per_module", module_block_arity),
            ("element_entries", element_entries),
            ("table_entries", table_entries),
            ("data_segments", data_segments),
            ("data_bytes", data_bytes),
            ("edges_times_depth_times_locals_per_module", sum_edl),
            ("exported_functions", exported),
            (
                "functions_total",
                bodies.len() as u64 + import_fn_types.len() as u64,
            ),
            ("ops_times_frames_per_module", sum_of),
            ("ops_times_locals_per_module", sum_ol),
            ("locals_per_module", sum_locals),
            ("locals_times_edges_per_module", sum_le),
            ("fn_arity_per_module", fn_arity),
            ("type_arity_per_module", type_arity.iter().sum()),
        ] {
            m.insert(k.into(), v.into());
        }
        m
    }

    /// The scan's counters agree with an independent count over every module in
    /// the tree and over generated modules that exercise each counter. A
    /// counter that stops updating, or counts twice, fails here even when no
    /// limit is near.
    #[test]
    fn the_scan_agrees_with_an_independent_count() {
        let modules: Vec<(&str, Vec<u8>)> = vec![
            (
                "website_contract",
                include_bytes!("../../../../fdev/resources/website_contract.wasm").to_vec(),
            ),
            (
                "test_contract_1",
                crate::wasm_runtime::tests::get_test_module("test_contract_1").unwrap(),
            ),
            ("nested", nested_blocks(50)),
            ("loops", loops(40)),
            ("ifs", ifs(30)),
            ("locals_and_blocks", locals_and_blocks(20, 30, 3)),
            ("branches", locals_and_branches(7, 100, 4)),
            ("br_tables", br_tables_to_a_loop(30, 5, 9)),
            ("body", locals_and_body_in_functions(5, 300, 3)),
            ("table_gets", table_gets(25)),
            ("call_indirects", call_indirects(25)),
            (
                "vector",
                wasm(&repeated_ops("(drop (v128.const i64x2 0 0))", 7)),
            ),
            ("arities", functions_with_params(5, 7)),
            ("types", types_with_params(4, 6)),
            ("frames_and_nops", frames_and_nops(6, 20, 4)),
        ];
        for (name, code) in modules {
            let scanned = as_map(&measure(&code).unwrap_or_else(|e| panic!("{name}: {e}")));
            for (field, want) in independent_shape(&code) {
                assert_eq!(scanned[&field], want, "{name}: {field}");
            }
        }
    }

    #[test]
    fn module_over_the_byte_limit_is_refused() {
        check_module(&padded_to(MAX_MODULE_BYTES)).expect("a module at the limit");
        assert!(matches!(
            check_module(&padded_to(MAX_MODULE_BYTES + 1)),
            Err(CompileRefusal::ModuleTooLarge { .. })
        ));
    }

    #[test]
    fn input_that_is_not_wasm_or_is_malformed_is_refused() {
        assert!(matches!(
            check_module(b"hello"),
            Err(CompileRefusal::NotBinary(_))
        ));
        assert!(matches!(
            check_module(b"\0asm\x01\0\0\0\xff\xff"),
            Err(CompileRefusal::Malformed(_))
        ));
    }

    /// No refusal carries more than the diagnostic bound, whatever the input.
    #[test]
    fn refusal_text_is_bounded() {
        let long = "x".repeat(10_000);
        let text = bounded_text(&long);
        assert!(text.len() <= MAX_DIAGNOSTIC_BYTES, "{}", text.len());
        assert!(text.ends_with("..."));
        // A multi-byte character is never cut in half.
        let wide = "\u{1F600}".repeat(1_000);
        let text = bounded_text(&wide);
        assert!(text.len() <= MAX_DIAGNOSTIC_BYTES);
        assert!(text.chars().filter(|c| *c != '.').all(|c| c == '\u{1F600}'));
        // Text under the bound is untouched.
        assert_eq!(bounded_text(&"short"), "short");
    }

    /// Text-format input is refused as not binary, before any conversion, with a
    /// small refusal however large the text is.
    #[test]
    fn text_format_input_is_refused_without_conversion() {
        for text in [
            "(module)".to_string(),
            loops_text(10),
            format!("(module (x{}", "\t".repeat(1024 * 1024)),
        ] {
            let refusal = check_module(text.as_bytes()).unwrap_err();
            assert!(matches!(refusal, CompileRefusal::NotBinary(_)), "{refusal}");
            assert!(refusal.to_string().len() < 1024);
        }
    }

    #[test]
    fn the_shape_of_an_ordinary_module_is_reported() {
        let shape = check_module(&locals_and_body(2, 5, 100)).unwrap();
        assert_eq!(shape.functions, 1);
        assert_eq!(shape.locals_plus_params_per_fn, 7);
        assert!(shape.body_bytes_per_fn >= 100);
    }

    /// The numeric value of every limit. Raising a limit is a change to the
    /// network-wide acceptance rule and is made here, deliberately, together
    /// with the measurement that justifies it (see `reference_shapes.json`); a
    /// build with a larger constant fails.
    #[test]
    fn every_limit_has_its_recorded_value() {
        let want: &[(&str, u64)] = &[
            ("module_bytes", 10 * 1024 * 1024),
            ("functions", 34_200),
            ("body_bytes_per_fn", 942_080),
            ("locals_plus_params_per_fn", 4_300),
            ("locals_times_body", 54_525_952),
            ("control_frames_per_fn", 22_700),
            ("control_frames_per_module", 211_000),
            ("control_frames_squared_per_module", 156_000_000),
            ("locals_times_frames_per_fn", 1_000_000),
            ("locals_times_frames_per_module", 5_900_000),
            ("branch_edges_per_fn", 41_400),
            ("branch_edges_squared_per_module", 1_800_000_000),
            ("nesting_depth", 1_024),
            ("br_table_targets", 4_096),
            ("block_type_arity", 32),
            ("block_type_arity_sum_per_fn", 4_096),
            ("fn_type_params", 96),
            ("fn_type_results", 16),
            ("operand_stack_height", 137),
            ("type_entries", 10_000),
            ("ops_per_fn", 410_000),
            ("weighted_ops_per_fn", 8_400_000),
            ("weighted_ops_per_module", 123_000_000),
            ("locals_per_module", 192_000),
            ("ops_times_locals_per_module", 3_500_000_000),
            ("ops_per_module", 5_100_000),
            ("ops_times_frames_per_module", 57_100_000_000),
            ("locals_times_edges_per_module", 9_500_000),
            ("fn_arity_per_module", 92_000),
            ("type_arity_per_module", 1_900),
            ("block_type_arity_per_module", 8_192),
            ("element_entries", 2_500),
            ("table_entries", 2_500),
            ("data_segments", 750),
            ("data_bytes", 10 * 1024 * 1024),
            ("ops_times_stack_per_module", 844_000_000),
            ("edges_times_depth_times_locals_per_module", 637_000_000),
            ("functions_total", 34_200),
            ("exported_functions", 17_000),
        ];
        let have = as_map(&ModuleShape::limits());
        assert_eq!(have.len(), want.len(), "a limit has no recorded value");
        let wrong: Vec<String> = want
            .iter()
            .filter(|(name, value)| have.get(*name).and_then(|v| v.as_u64()) != Some(*value))
            .map(|(name, value)| format!("{name}: recorded {value}, built {:?}", have.get(*name)))
            .collect();
        assert!(wrong.is_empty(), "limits changed: {wrong:#?}");
    }

    /// Every SIMD operator, relaxed SIMD included, weighs as much as a load.
    #[test]
    fn vector_operators_weigh_as_much_as_a_load() {
        let weighted = |wat_unit: &str, n: usize| {
            measure(&wasm(&repeated_ops(wat_unit, n)))
                .unwrap()
                .weighted_ops_per_fn
        };
        // A constant pair is 2 operators; a vector constant plus drop is 96 + 1.
        let plain = |n| weighted("(drop (i32.const 0))", n);
        let vector = |n| weighted("(drop (v128.const i64x2 0 0))", n);
        assert_eq!(plain(11) - plain(10), 2);
        assert_eq!(vector(11) - vector(10), 97);
        let relaxed = measure_with_all_features(&wasm(
            "(module (func (param v128) (result v128) (i8x16.relaxed_swizzle (local.get 0) (local.get 0))))",
        ));
        let plain_shape = measure_with_all_features(&wasm(
            "(module (func (param i32) (result i32) (i32.add (local.get 0) (local.get 0))))",
        ));
        assert_eq!(
            relaxed.weighted_ops_per_fn,
            plain_shape.weighted_ops_per_fn + 95
        );
    }

    fn measure_with_all_features(code: &[u8]) -> ModuleShape {
        scan_with(code, false, WasmFeatures::all()).unwrap()
    }

    /// Every operator that makes the code generator split blocks counts as one
    /// control frame, so a function made of them hits the frame limits like one
    /// made of loops.
    #[test]
    fn block_creating_operators_count_as_frames() {
        let cases: &[(&str, &str)] = &[
            ("table.get", "(drop (table.get 0 (i32.const 0)))"),
            ("table.set", "(table.set 0 (i32.const 0) (ref.null func))"),
            (
                "table.grow",
                "(drop (table.grow 0 (ref.null func) (i32.const 0)))",
            ),
            (
                "table.fill",
                "(table.fill 0 (i32.const 0) (ref.null func) (i32.const 0))",
            ),
            (
                "table.copy",
                "(table.copy 0 0 (i32.const 0) (i32.const 0) (i32.const 0))",
            ),
            (
                "table.init",
                "(table.init 0 $e (i32.const 0) (i32.const 0) (i32.const 0))",
            ),
            ("call_indirect", "(call_indirect (type $t) (i32.const 0))"),
        ];
        for (name, op) in cases {
            let text = format!(
                "(module (type $t (func)) (table 1 funcref) (elem $e func) (func (export \"f\") {}))",
                op.repeat(3)
            );
            let shape = measure(&wasm(&text)).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(shape.control_frames_per_fn, 3, "{name}");
        }
        // Operators of proposals the accepted set refuses are counted too.
        let extra: &[(&str, &str)] = &[
            (
                "return_call_indirect",
                "(module (type $t (func)) (table 1 funcref) (func (export \"f\") (return_call_indirect (type $t) (i32.const 0))))",
            ),
            (
                "call_ref",
                "(module (type $t (func)) (func $g) (elem declare func $g) (func (export \"f\") (call_ref $t (ref.func $g))))",
            ),
            (
                "return_call_ref",
                "(module (type $t (func)) (func $g) (elem declare func $g) (func (export \"f\") (return_call_ref $t (ref.func $g))))",
            ),
            (
                "ref.as_non_null",
                "(module (func (export \"f\") (param funcref) (drop (ref.as_non_null (local.get 0)))))",
            ),
        ];
        for (name, text) in extra {
            let shape = measure_with_all_features(&wasm(text));
            assert_eq!(shape.control_frames_per_fn, 1, "{name}");
        }
    }

    /// The generic operator-times-frames bound counts block-creating
    /// operators.
    #[test]
    fn the_operator_bound_sees_block_creating_operators() {
        let shape = measure(&table_gets(100)).unwrap();
        // 100 frames, each (i32.const, table.get, drop) = 3 operators, plus end;
        // table.get weighs 2048, so a unit weighs 2050.
        assert_eq!(shape.ops_per_fn, 301);
        assert_eq!(shape.weighted_ops_per_fn, 100 * 2_050 + 1);
        assert_eq!(shape.ops_times_frames_per_module, (100 * 2_050 + 1) * 100);
    }

    /// The weight of each class of operator: the weighted-operator count of
    /// `n + 1` units of `unit` minus that of `n`.
    fn unit_weight(module: &str, unit: &str) -> u64 {
        let build = |n: usize| {
            let text = module.replace("UNITS", &unit.repeat(n));
            scan_with(&wasm(&text), false, WasmFeatures::all())
                .unwrap_or_else(|e| panic!("{unit}: {e}"))
                .weighted_ops_per_fn
        };
        build(11) - build(10)
    }

    const FN_MODULE: &str = "(module (memory 1) (table 4 funcref) (global (mut i32) (i32.const 0)) (data $d \"x\") (elem $e func $g) (type $t (func)) (func $g) (func (export \"f\") (param i32 i64 f32 f64) UNITS))";

    /// Each class has its weight: cheap operators 1; loads, stores,
    /// calls, globals, division and every SIMD operator 96; everything else
    /// 2048.
    #[test]
    fn operators_have_the_weight_of_their_class() {
        let cheap: &[(&str, u64)] = &[
            ("(drop (i32.add (local.get 0) (local.get 0)))", 4),
            ("(drop (i64.mul (local.get 1) (local.get 1)))", 4),
            ("(drop (f64.div (local.get 3) (local.get 3)))", 4),
            ("(drop (f64.sqrt (local.get 3)))", 3),
            ("(drop (i32.wrap_i64 (local.get 1)))", 3),
            ("(drop (i32.trunc_sat_f32_s (local.get 2)))", 3),
            (
                "(drop (select (local.get 0) (local.get 0) (local.get 0)))",
                5,
            ),
            ("(local.set 0 (local.tee 0 (i32.const 1)))", 3),
            ("(drop (ref.is_null (ref.null func)))", 3),
        ];
        let common: &[(&str, u64)] = &[
            ("(drop (i32.load (i32.const 0)))", 98),
            ("(drop (i64.load offset=4294967295 (i32.const 0)))", 98),
            ("(i32.store (i32.const 0) (i32.const 0))", 98),
            ("(i64.store8 (i32.const 0) (i64.const 0))", 98),
            ("(call $g)", 96),
            ("(drop (global.get 0))", 97),
            ("(global.set 0 (i32.const 0))", 97),
            ("(drop (i32.div_u (local.get 0) (local.get 0)))", 99),
            ("(drop (i64.rem_s (local.get 1) (local.get 1)))", 99),
            ("(drop (i32.trunc_f64_s (local.get 3)))", 98),
            ("(drop (memory.size))", 97),
            ("(drop (table.size 0))", 97),
            ("(data.drop $d)", 96),
            ("(elem.drop $e)", 96),
            ("(drop (ref.func $g))", 97),
            ("(drop (v128.load (i32.const 0)))", 98),
            ("(v128.store (i32.const 0) (v128.const i64x2 0 0))", 193),
        ];
        let vector: &[(&str, u64)] = &[
            ("(drop (v128.const i64x2 0 0))", 97),
            (
                "(drop (i64x2.mul (v128.const i64x2 0 0) (v128.const i64x2 0 0)))",
                289,
            ),
            ("(drop (i8x16.popcnt (v128.const i64x2 0 0)))", 193),
        ];
        let heavy: &[(&str, u64)] = &[
            ("(drop (memory.grow (i32.const 0)))", 2_050),
            (
                "(memory.fill (i32.const 0) (i32.const 0) (i32.const 0))",
                2_051,
            ),
            (
                "(memory.copy (i32.const 0) (i32.const 0) (i32.const 0))",
                2_051,
            ),
            (
                "(memory.init $d (i32.const 0) (i32.const 0) (i32.const 0))",
                2_051,
            ),
            ("(drop (table.get 0 (i32.const 0)))", 2_050),
            ("(table.set 0 (i32.const 0) (ref.null func))", 2_050),
            ("(drop (table.grow 0 (ref.null func) (i32.const 0)))", 2_051),
            (
                "(table.fill 0 (i32.const 0) (ref.null func) (i32.const 0))",
                2_051,
            ),
            (
                "(table.copy 0 0 (i32.const 0) (i32.const 0) (i32.const 0))",
                2_051,
            ),
            (
                "(table.init 0 $e (i32.const 0) (i32.const 0) (i32.const 0))",
                2_051,
            ),
            ("(call_indirect (type $t) (i32.const 0))", 2_049),
        ];
        let mut wrong = Vec::new();
        for (class, rows) in [
            ("cheap", cheap),
            ("common", common),
            ("vector", vector),
            ("heavy", heavy),
        ] {
            for (unit, expect) in rows {
                let got = unit_weight(FN_MODULE, unit);
                if got != *expect {
                    wrong.push(format!("{class}: {unit}: weight {got}, expected {expect}"));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// An operator the model does not name gets the heavy weight, not the
    /// cheap one.
    #[test]
    fn an_operator_the_model_does_not_name_is_heavy() {
        let module = "(module (memory 1 1 shared) (func (export \"f\") UNITS))";
        // atomic.fence is decodable, takes and returns nothing, and is not in
        // any list: one operator, heavy weight.
        assert_eq!(unit_weight(module, "(atomic.fence)"), WEIGHT_HEAVY);
        assert_eq!(
            unit_weight(module, "(drop (i32.atomic.load (i32.const 0)))"),
            WEIGHT_HEAVY + 2
        );
        assert_eq!(
            unit_weight(
                module,
                "(drop (memory.atomic.notify (i32.const 0) (i32.const 0)))"
            ),
            WEIGHT_HEAVY + 3
        );
    }

    /// This pins the ordering of the three weights.
    #[test]
    fn heavy_operators_outweigh_common_ones_and_common_outweigh_cheap() {
        const {
            assert!(WEIGHT_CHEAP < WEIGHT_COMMON);
            assert!(WEIGHT_COMMON < WEIGHT_HEAVY);
        }
        // The weights are pinned by value: a model change is a decision.
        assert_eq!((WEIGHT_CHEAP, WEIGHT_COMMON, WEIGHT_HEAVY), (1, 96, 2_048));
        assert_eq!(OPERATOR_MODEL_VERSION, 3);
    }

    /// The operator model is part of the limits fingerprint.
    #[test]
    fn the_operator_model_is_in_the_limits_fingerprint() {
        let base = ModuleShape::limits();
        let current = fingerprint_of(&base, 1);
        assert_eq!(current, fingerprint_with_model(&base, 1, &operator_model()));
        for other in [
            "2|1,4,96,2048",
            "3|1,4,96,2047",
            "3|1,4,95,2048",
            "3|2,4,96,2048",
        ] {
            assert_ne!(current, fingerprint_with_model(&base, 1, other), "{other}");
        }
        assert_eq!(operator_model(), "3|1,96,2048");
    }

    /// Every form of element segment is counted, entries of function-index and
    /// expression form alike, active, passive and declarative.
    #[test]
    fn every_form_of_element_segment_is_counted() {
        let forms: &[(&str, &str, u64)] = &[
            ("active funcs", "(elem (i32.const 0) func $f $f $f)", 3),
            (
                "active exprs",
                "(elem (i32.const 0) funcref (ref.func $f) (ref.func $f))",
                2,
            ),
            ("passive funcs", "(elem func $f $f $f $f)", 4),
            (
                "passive exprs",
                "(elem funcref (ref.func $f) (ref.null func))",
                2,
            ),
            ("declarative", "(elem declare func $f $f $f)", 3),
            ("empty", "(elem func)", 0),
        ];
        for (name, segment, want) in forms {
            let text = format!("(module (table 8 funcref) (func $f) {segment})");
            let shape = measure(&wasm(&text)).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(shape.element_entries, *want, "{name}");
        }
        // Several segments add up.
        let many = "(module (table 8 funcref) (func $f) (elem func $f $f) (elem func $f) (elem (i32.const 0) func $f $f))";
        assert_eq!(measure(&wasm(many)).unwrap().element_entries, 5);
    }

    /// The entry limits are far below wasmtime's own static-initialisation
    /// threshold, and modules over it are refused by the entry counts.
    #[test]
    fn the_entry_limits_are_below_the_static_initialisation_threshold() {
        const {
            assert!(MAX_MODULE_ELEMENT_ENTRIES * 64 < 1 << 20);
            assert!(MAX_MODULE_TABLE_ENTRIES * 64 < 1 << 20);
        }
        for n in [(1usize << 20) - 1, (1 << 20) + 1, 2_000_000] {
            assert_eq!(
                check_module(&element_entries_of(n)),
                Err(CompileRefusal::TooManyElementEntries {
                    limit: MAX_MODULE_ELEMENT_ENTRIES
                }),
                "{n} element entries"
            );
            assert_eq!(
                check_module(&table_entries_of(n)),
                Err(CompileRefusal::TooManyTableEntries {
                    limit: MAX_MODULE_TABLE_ENTRIES
                }),
                "{n} table entries"
            );
        }
    }

    /// Data segments and their bytes are counted.
    #[test]
    fn data_segments_and_bytes_are_counted() {
        let shape = measure(&wasm(
            "(module (memory 1) (data (i32.const 0) \"abc\") (data \"de\") (data (i32.const 8) \"\"))",
        ))
        .unwrap();
        assert_eq!(shape.data_segments, 3);
        assert_eq!(shape.data_bytes, 5);
    }

    /// A function of block-splitting operators is refused by the weighted
    /// operator limit before the frame limit.
    #[test]
    fn heavy_block_operators_are_refused_by_weight_before_frames() {
        // What one repeated unit weighs.
        let per_unit = measure(&table_gets(2)).unwrap().weighted_ops_per_fn
            - measure(&table_gets(1)).unwrap().weighted_ops_per_fn;
        let fits = (MAX_FUNCTION_WEIGHTED_OPS / per_unit) as usize;
        assert!(fits < MAX_FUNCTION_CONTROL_FRAMES as usize);
        let ci_unit = measure(&call_indirects(2)).unwrap().weighted_ops_per_fn
            - measure(&call_indirects(1)).unwrap().weighted_ops_per_fn;
        let ci_fits = (MAX_FUNCTION_WEIGHTED_OPS / ci_unit) as usize;
        assert!(check_module(&table_gets(fits - 1)).is_ok());
        assert_eq!(
            check_module(&table_gets(fits + 1)),
            Err(CompileRefusal::TooManyWeightedOps {
                limit: MAX_FUNCTION_WEIGHTED_OPS
            })
        );
        assert_eq!(
            check_module(&call_indirects(ci_fits + 1)),
            Err(CompileRefusal::TooManyWeightedOps {
                limit: MAX_FUNCTION_WEIGHTED_OPS
            })
        );
    }

    /// The operand stack cap triggers on a generated module: a function that
    /// holds one value more than the cap is refused, one at the cap is not.
    #[test]
    fn the_operand_stack_height_cap_triggers() {
        let cap = MAX_OPERAND_STACK_HEIGHT as usize;
        assert!(check_module(&tall_stack(cap)).is_ok());
        assert_eq!(
            check_module(&tall_stack(cap + 1)),
            Err(CompileRefusal::OperandStackTooTall {
                limit: MAX_OPERAND_STACK_HEIGHT
            })
        );
        // A tall stack of distinct constants is refused by the cap.
        let consts: String = (0..4_000)
            .map(|i| format!("(i64.const {})", (1u64 << 33) + i))
            .collect();
        let sums = "(i64.add)".repeat(3_999);
        let deep = wasm(&format!(
            "(module (func (export \"f\") (result i64) {consts}{sums}))"
        ));
        assert_eq!(
            check_module(&deep),
            Err(CompileRefusal::OperandStackTooTall {
                limit: MAX_OPERAND_STACK_HEIGHT
            })
        );
    }

    /// The cap keeps the 4x margin over the deployed modules the other limits
    /// have.
    #[test]
    fn the_operand_stack_cap_is_at_least_four_times_the_deployed_maximum() {
        const { assert!(MAX_OPERAND_STACK_HEIGHT >= 4 * 23) };
    }

    /// The scan and the engine parse with one `wasmparser`: the lock file must
    /// carry a single version for the two consumers.
    #[test]
    fn scan_and_engine_use_one_wasmparser() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock");
        let Ok(lock) = std::fs::read_to_string(path) else {
            // A packaged crate has no workspace lock file to read.
            return;
        };
        // `[[package]]` blocks: name, version, dependencies.
        let mut ours = Vec::new();
        let mut theirs = Vec::new();
        for block in lock.split("[[package]]") {
            let name = block
                .lines()
                .find_map(|l| l.strip_prefix("name = \""))
                .map(|l| l.trim_end_matches('"'));
            let wp: Vec<String> = block
                .lines()
                .filter_map(|l| l.trim().strip_prefix('"'))
                .filter(|l| l.starts_with("wasmparser"))
                .map(|l| l.trim_end_matches(['"', ',']).to_string())
                .collect();
            match name {
                Some("freenet") => ours = wp,
                Some("wasmtime-environ") => theirs = wp,
                _ => {}
            }
        }
        let versions: Vec<String> = lock
            .split("[[package]]")
            .filter(|b| b.contains("name = \"wasmparser\""))
            .filter_map(|b| {
                b.lines()
                    .find_map(|l| l.strip_prefix("version = \""))
                    .map(|v| v.trim_end_matches('"').to_string())
            })
            .collect();
        let resolve = |dep: &[String]| -> String {
            let dep = dep.first().expect("a wasmparser dependency");
            match dep.split_once(' ') {
                Some((_, v)) => v.to_string(),
                None => {
                    assert_eq!(versions.len(), 1, "unqualified dependency, several copies");
                    versions[0].clone()
                }
            }
        };
        assert_eq!(
            resolve(&ours),
            resolve(&theirs),
            "the scan's wasmparser is not the one wasmtime compiles with: {versions:?}"
        );
    }
}
