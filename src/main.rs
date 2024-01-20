use analyzer::Analyzer;
use clap::Parser;

mod analyzer;
mod parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File to read.
    #[arg(short, long)]
    input: String,

    /// Don't print anything.
    #[arg(short, long)]
    quiet: bool,

    /// Don't print anything.
    #[arg(short, long)]
    method: String,
}

fn main() {
    let args = Args::parse();

    let path = std::path::Path::new(&args.input);

    if !path.exists() {
        eprintln!("no such file: {}", args.input);
        std::process::exit(1);
    }

    let results = match args.method.as_str() {
        "mmap" => Analyzer::<parser::mmap_source::MmapIterator>::new(path).collect(),
        "fd" => Analyzer::<parser::fd_source::FdIterator>::new(path).collect(),
        _ => {
            eprintln!("invalid method: {}", args.method);
            std::process::exit(1);
        }
    };

    if !args.quiet {
        print!("{{");
        let mut first = true;
        for r in results {
            if !first {
                print!(", ");
            }
            first = false;
            print!("{}={}/{}/{}", r.name, r.min, r.mean, r.max);
        }
        println!("}}");
    }
}
