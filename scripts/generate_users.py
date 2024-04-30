import sys
import csv
import decimal
import random
import datetime
import argparse

from faker import Faker
from faker.providers import BaseProvider


class UserDataProvider(BaseProvider):

    def date(self):
        start_date = datetime.date(2018, 1, 1)
        return start_date + datetime.timedelta(random.randint(1, 365))

    def timestamp(self):
        date = self.date()
        time = datetime.time(
            random.randint(0, 23),
            random.randint(0, 59),
            random.randint(0, 59),
            random.randint(0, 999) * 1000
        )
        return datetime.datetime.combine(date, time)

    def boolean(self):
        return self.random_element([True, False])

    def status(self):
        return self.random_element(
            ['ACTIVE', 'PENDING', 'SUSPENDED', 'DISABLED']
        )

    def decimal(self):
        return decimal.Decimal(random.randint(0, 100)) / 100

    def score(self):
        value = random.randint(0, 10)
        return None if value == 10 else random.randint(0, 10000) / 100


def generate_users(count):
    fake = Faker()
    fake.add_provider(UserDataProvider)

    for i in range(count):
        yield (
            i,
            fake.name(),
            fake.date(),
            fake.timestamp(),
            fake.boolean(),
            fake.decimal(),
            fake.score(),
            fake.status()
        )


def _create_parser():
    parser = argparse.ArgumentParser(
        description="Generate a CSV file containing users",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'filename',
        type=argparse.FileType('w', encoding='utf8'),
        help='file the resulting CSV should be written to.'
    )
    parser.add_argument(
        '-n', '--count',
        type=int, default=10000,
        help='Amount of users to create.'
    )

    return parser


FAILURE = -1
SUCCESS = 0


def main(argv=None):
    parser = _create_parser()
    args = parser.parse_args(argv)
    try:
        writer = csv.writer(args.filename, delimiter=',')
        for user in generate_users(count=args.count):
            writer.writerow(user)
    except Exception as ex:
        print(f"Error while generating users, details: {ex}")
        return FAILURE
    return SUCCESS


if __name__ == '__main__':
    sys.exit(main())
