import argparse
import csv
import datetime
import decimal
import random
import sys
from pathlib import Path

from faker import Faker
from faker.providers import BaseProvider


class PaymentsProvider(BaseProvider):

    def date(self):
        start_date = datetime.date(2018, 1, 1)
        return start_date + datetime.timedelta(random.randint(1, 365))

    def timestamp(self):
        date = self.date()
        time = datetime.time(
            random.randint(0, 23),
            random.randint(0, 59),
            random.randint(0, 59),
            random.randint(0, 999) * 1000,
        )
        return datetime.datetime.combine(date, time)

    def amount(self):
        gross = decimal.Decimal(random.randint(0, 1000)) / 100
        net = gross * decimal.Decimal("0.7")
        return gross, net

    def user_id(self, min=1, max=10000):
        return random.randint(min, max)

    def payment_id(self):
        return "-".join(
            [
                str(random.randint(100, 300)),
                str(random.randint(100, 300)),
                str(random.randint(100, 300)),
            ]
        )


def generate_payments(count, min, max):
    fake = Faker()
    fake.add_provider(PaymentsProvider)

    for i in range(count):
        gross, net = fake.amount()
        yield (fake.user_id(min, max), fake.payment_id(), fake.timestamp(), gross, net)


def _create_parser():
    parser = argparse.ArgumentParser(
        description="Generate a CSV file containing payments",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "filename", type=Path, help="file the resulting CSV should be written to."
    )
    parser.add_argument(
        "-n", "--count", type=int, default=10000, help="Number of users to create."
    )
    parser.add_argument(
        "--min", type=int, default=1, help="Lower bound for user_id used in payments."
    )
    parser.add_argument(
        "--max",
        type=int,
        default=10000,
        help="Upper bound for user_id used in payments.",
    )

    return parser


FAILURE = -1
SUCCESS = 0


def main(argv=None):
    parser = _create_parser()
    args = parser.parse_args(argv)
    try:
        with open(args.filename, "w", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            for user in generate_payments(count=args.count, min=args.min, max=args.max):
                writer.writerow(user)
    except Exception as ex:
        print(f"Error while generating users, details: {ex}")
        return FAILURE
    return SUCCESS


if __name__ == "__main__":
    sys.exit(main())
