import re
from pathlib import Path


def analyze_file(file_input):
    regex = re.compile(r'(\[[0-9 -:,]+\])\s([a-zA-Z0-9.:{}_]+)\s([a-zA-Z]+)\s(-)\s([a-zA-Z0-9+\s<:_.-]+)')

    with open(file_input, "r") as file:
        content = file.read()
        matches = regex.finditer(content)
        error_list = []
        ctr = 0
        for match in matches:
            if match[3] == 'ERROR':
                ctr += 1
                error_list.append(match[0])

        return ctr, error_list


def main():
    log_dir = "c:\\GoogleDrive\\Springboard\\airflow-mini-project\\logs\\MarketVol"

    file_list = Path(log_dir).rglob('*.log')
    total_err_cnt = 0
    error_list = []
    for file in file_list:
        cnt, cur_list = analyze_file(file)

        total_err_cnt = total_err_cnt + cnt
        error_list = error_list + cur_list

    print('Total number of errors: {}'.format(total_err_cnt))
    print('\n')
    print('Here are all the errors:')
    for err in error_list:
        print(err)


if __name__ == '__main__':
    main()
