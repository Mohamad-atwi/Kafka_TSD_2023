#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    consumer = Consumer(config)

    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    topic = "grades"
    consumer.subscribe([topic], on_assign=reset_offset)

    course_grades = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                course_name = msg.key().decode('utf-8')
                grade = int(msg.value().decode('utf-8'))
                course_grades[course_name] = grade

                total_grades = sum(course_grades.values())
                gpa = total_grades / len(course_grades)

                print("Course: {}, Grade: {}".format(course_name, grade))
                print("Current GPA: {}".format(gpa))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()