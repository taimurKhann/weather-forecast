import sys
import pipeline.pipeline as pl


batch_type_list = ['daily', 'historical']

usage_text = """
Usage of Script: main.py [ daily | historical ]
"""


def main():
    args_len = len(sys.argv)
    batch_type = sys.argv[0]
    
    if args_len == 2:
        batch_type = sys.argv[1]
        if batch_type not in batch_type_list:
            print(usage_text)
            return 0
    else:
        print(usage_text)
        return 0

    pl.run(batch_type)


if __name__ == '__main__':
    main()