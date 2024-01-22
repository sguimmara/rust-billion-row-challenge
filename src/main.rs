use clap::Parser;

mod processor;
mod parser;

use parser::{ChunkParser, MemoryMappedParser};
use processor::SequentialProcessor;

#[derive(clap::ValueEnum, Clone, Default, Debug, PartialEq)]
#[clap(rename_all = "kebab_case")]
enum ParserType {
    #[default]
    Chunk,
    MemoryMapped,
}


#[derive(clap::ValueEnum, Clone, Default, Debug, PartialEq)]
#[clap(rename_all = "kebab_case")]
enum ProcessorType {
    #[default]
    Sequential,
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The file to read.
    #[arg(short, long)]
    input: String,

    /// Don't print anything.
    #[arg(short, long)]
    quiet: bool,

    /// How the input CSV file is parsed into rows
    #[arg(long)]
    parser: ParserType,

    /// How the CSV rows are processed.
    #[arg(long)]
    processor: ProcessorType,
}

fn main() {
    let args = Args::parse();

    let path = std::path::Path::new(&args.input);

    if !path.exists() {
        eprintln!("no such file: {}", args.input);
        std::process::exit(1);
    }

    let results = match (args.processor, args.parser) {
        (ProcessorType::Sequential, ParserType::Chunk) => SequentialProcessor::<ChunkParser>::new(path).collect(),
        (ProcessorType::Sequential, ParserType::MemoryMapped) => SequentialProcessor::<MemoryMappedParser>::new(path).collect(),
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
