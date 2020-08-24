## Warnings
Code should pass `cargo clippy` without warnings. If a warning is not useful, allow it with `#[allow(clippy::...)]`.

## Formatting
Code should be formatted with `cargo fmt`.

## Imports
Most files should `use super::*`, which will pull in all public names in the project, and the library and standard library names we commonly use (see [src/main.rs](src/main.rs). Library and standard library names we use only a few places should be `use`d in those files only. Modules should only `pub use` what is needed outside of their module. All names should be unique within the scope of where they're used (that is two sibling modules can have private structs with conflicting names, but if either is public then one or both of the names should be changed).