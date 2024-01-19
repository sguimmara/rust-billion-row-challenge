use clap::Parser;

mod parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File to read
    #[arg(short, long)]
    input: String,
}

fn main() {
    let args = Args::parse();

    let path = std::path::Path::new(&args.input);

    if !path.exists() {
        eprintln!("no such file: {}", args.input);
        std::process::exit(1);
    }
}