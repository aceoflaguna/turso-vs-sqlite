1. **Install Rust** (if you don't have it) from https://rustup.rs. Check with `cargo --version`.
2. **Create the project layout** from the files I sent:
   ```
   dbbench/
   ├── Cargo.toml
   └── src/
       ├── main.rs
       └── report_template.html
   ```
   `main.rs` and `report_template.html` must both be inside `src/`.
3. **Run it** from the `dbbench` folder:
   ```
   cargo run --release
   ```
   The first build takes a few minutes because it compiles SQLite and Turso. Always use `--release`, or the numbers will be meaningless.
4. **Open `report.html`** in your browser. It's written to the folder you ran the command from, next to `results.json`.

**Useful options** (put them after `--`):
- `cargo run --release -- --quick` is a 30-second smoke test.
- `--dir D:\bench` puts the scratch databases on the disk you actually care about. By default they go in your temp folder, which may be RAM-backed or a different drive.
- `--synchronous full` makes every commit fsync, which is the realistic durability test and the one where write numbers differ most.
- `--rows 1000000` gives a bigger dataset.
- `--engines sqlite,turso` skips the MVCC variant.
- `--concurrency 1,4,16 --duration 10` sets the worker counts and seconds per level.

**Windows note:** if the build fails with a C compiler error, install "Build Tools for Visual Studio" with the C++ workload. The bundled SQLite needs it.

For fair results, close heavy apps while it runs, plug in a laptop, and run it two or three times, because single runs are noisy.