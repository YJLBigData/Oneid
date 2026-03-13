import argparse

from generate_user_data import DEFAULT_PERSONS, DEFAULT_SEED, generate_and_load
from init_schema import main as schema_main
from run_oneid import main as oneid_main
from stats_report import main as stats_main


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the complete local OneID pipeline.")
    parser.add_argument("--persons", type=int, default=DEFAULT_PERSONS, help="Number of natural persons to generate.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed for reproducible data.")
    parser.add_argument("--batch-size", type=int, default=500, help="MySQL insert batch size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    schema_main()
    generate_and_load(persons=args.persons, seed=args.seed, batch_size=args.batch_size)
    oneid_main()
    stats_main()


if __name__ == "__main__":
    main()
