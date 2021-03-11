import sys
import csv
from datetime import datetime as dt


class CsvQuery:

    def __init__(self, command, delimiter, encoding, date_format):
        KEYWORDS = {
            'SELECT': self.parse_fields,
            'FROM': self.parse_tablename,
            'WHERE': self.parse_condition,
            'LIKE': self.parse_like,
            'GROUP': self.parse_group,
            'ORDER': self.parse_sort,
            'LIMIT': self.parse_limit,
        }
        self.delimiter = delimiter
        self.encoding = encoding
        self.date_format = date_format
        self.adjust_format()
        self.reader = None
        self.field_list = {}
        self.conditions = {
            'fields': [],
            'values': '',
        }
        self.sort_by = ''
        self.group_field = ''
        self.limit = 10
        self.size_of = {}
        self.parse_function = None
        self.reverse_sorting = False
        self.all_fields = False
        for word in command.split(' '):
            if not word:
                continue
            new_function = KEYWORDS.get(word.upper())
            if new_function:
                self.parse_function = new_function
            elif self.parse_function:
                self.parse_function(word)
            else:
                raise Exception(f'Unknown "{word}" ..!!')

    def adjust_format(self):
        ELEMENT_MASK = {
            'd': '%d',
            'm': '%m',
            'y': '%Y'
        }
        result = ''
        for separator in ['/', '-']:
            elements = self.date_format.lower().split(separator)
            if len(elements) < 3:
                continue
            for e in elements:
                if result:
                    result += separator
                result += ELEMENT_MASK[e]
        self.date_format = result

    def parse_fields(self, param):
        if param == '*':
            self.all_fields = True
            return
        field = param.replace(',', '')
        self.size_of[field] = len(field) + 3
        func_type = ''
        if '(' in field:
            func_type, field = field.split('(')
            field = field.replace(')', '')
        # --------------------------------------
        self.field_list.setdefault(
            field, []
        ).append(
            func_type.lower()
        )
        # --------------------------------------

    def parse_tablename(self, param):
        self.reader = csv.DictReader(
            open(param, 'r', 
                encoding=self.encoding
            ),delimiter=self.delimiter
        )
        if self.all_fields:
            names = self.reader.fieldnames
            self.field_list = {f: '' for f in names}

    def parse_condition(self, param):
        elements = param.split('=')
        if len(elements) > 1:
            field = elements[0].split('.')[0]
            self.conditions['fields'].append(field)
            self.conditions['values'] += '{}=={}'.format(
                elements[0],
                elements[-1]
            )
            return
        value = param
        if param in ['AND', 'OR', 'and', 'or', 'IN', 'in']:
            value = param.lower()
        elif param == '=':
            value = '=='
        elif param.isidentifier():
            self.conditions['fields'].append(param)
        elif '.' in param:
            self.conditions['fields'].append(
                param.split('.')[0]
            )
        if self.conditions['values']:
            value = ' ' + value
        self.conditions['values'] += value

    def parse_like(self, param):
        words = self.conditions['values'].split(' ')
        self.conditions['values'] = '{} {} in {}'.format(
            ' '.join(words[:-1]),
            param.replace('%', ''),
            self.conditions['fields'][-1]
        )
        self.parse_function = self.parse_condition

    def parse_group(self, param):
        param = param.replace(' ', '')
        if param.upper() == 'BY':
            self.limit = 0
            return
        self.group_field = param

    def parse_sort(self, param):
        word = param.upper()
        if word == 'BY':
            return
        if self.sort_by and word == 'DESC':
            self.reverse_sorting = True
            return
        self.sort_by = param

    def parse_limit(self, param):
        self.limit = int(param)

    def filtered_row(self, row):
        if not self.conditions['values']:
            return True
        for field in self.conditions['fields']:
            value = self.try_numeric(row[field])
            if isinstance(value, str):
                if '.' in self.conditions['values']:
                    return False  # -- Expected Type = DATE
                func = self.field_list[field][-1]
                if self.group_field and func:
                    return False # --- Aggregation function (no-String type)
            exec(f"{field} = value")
        return eval(self.conditions['values'])

    def aggregate(self, group):
        AGG_FUNCS = {
            'max': max,
            'min': min,
            'sum': sum,
            'count': len,
            'avg': lambda x: sum(x) / len(x)
        }
        result = []
        for key, values in group.items():
            record = {}
            for field in self.field_list:
                is_group = field == self.group_field
                for func in self.field_list[field]:
                    if not func and is_group:
                        record[field] = key
                        continue
                    value = values[field]
                    size = self.size_of[field]
                    if func == 'count':
                        new = func
                    else:
                        new = f'{func}_{field}'
                    self.size_of[new] = max(size, len(new)) + 5
                    record[new] = AGG_FUNCS[func](value)
            result.append(record)
        self.field_list = {f: '' for f in record}
        return result

    def scan(self):
        result = []
        group = {}
        count = 0
        if self.field_list.pop('*', None):
            self.field_list.setdefault(
                self.group_field, []
            ).append('count')
            self.size_of[self.group_field] = 10
        for row in self.reader:
            if not self.filtered_row(row):
                continue
            record = {}
            if self.group_field:
                key = row[self.group_field]
            for field in self.field_list:
                value = row[field]
                curr_size = len(str(value))
                if curr_size > self.size_of.get(field, 0):
                    self.size_of[field] = curr_size + 5
                if self.group_field:
                    group.setdefault(key, {}).setdefault(field, []).append(
                        self.try_numeric(value)
                    )
                else:
                    record[field] = value
            if record:
                result.append(record)
            count += 1
            if count == self.limit:
                break        
        if group:
            result = self.aggregate(group)
        if self.sort_by:
            if self.sort_by.isnumeric():
                i = int(self.sort_by)
                self.sort_by = list(self.field_list)[i-1]
            result = sorted(result, key=lambda k: k[self.sort_by])
            if self.reverse_sorting: result = result[::-1]
        return result

    def run(self):
        def truncate(s, size):
            s += ' ' * size
            return s[:size]
        head = '\n'
        line = ''
        dataset = self.scan()
        for field, size in self.size_of.items():
            if field not in self.field_list:
                continue
            head += truncate(field, size)
            line += '-' * (size-3) + '-+-'
        print(head)
        print(line)
        for row in dataset:
            line = ''
            for field in self.field_list:
                if field not in self.size_of:
                    size = 10
                else:
                    size = self.size_of[field]
                line += truncate(
                    str(row[field]),
                    size-3
                ) + ' | '
            print(line)

    def try_numeric(self, expr, separator='.', type_class=float):
        candidate = expr.replace(' ', '')
        elements = candidate.split(separator)
        if candidate.startswith('-'):
            is_number = elements[0].strip('-').isnumeric()
        else:
            is_number = elements[0].isnumeric()
        if is_number:
            if len(elements) > 1:
                return type_class(candidate)
            else:
                return int(candidate)
        if separator == '.':
            for char in [s for s in ['/', '-'] if s in expr]:
                return self.try_numeric( 
                    expr, char,
                    lambda s: dt.strptime(s, self.date_format)
                )
        return expr   


def extract_args():
    FLAGS = {
        '-d': ('delimiter', ','),
        '-e': ('encoding', None),
        '-f': ('date_format', 'y-m-d'),
    }
    ignore = True # -- ignore  sys.argv[0]
    key = ''
    result = {f[0]: f[1] for f in FLAGS.values()}
    for arg in sys.argv:
        if ignore:
            ignore = False
            continue
        if arg in FLAGS:
            key = FLAGS[arg][0]
        elif key:
            result[key] = arg
            key = ''
        else:
            result['command'] = arg
    return result

if __name__ == '__main__':
    if len(sys.argv) > 1:
        params = extract_args()
        query = CsvQuery(**params)
        query.run()
    else:
        print('''
        '* * *  QR 1.2021.03.11 r 11.25  * * * '

        How to use:
            > python qr.py "<command>" [-d ...] [-e ...] [-f ...]
            options:
                -d <delimiter>
                -e <encoding>
                -f <date format>
        ''')
    # ------------- Exemplos: ------------------------------
    # python qr.py "SELECT nome, idade FROM pessoas.csv WHERE idade < 35 AND sexo = 'F' ORDER BY nome LIMIT 20"
    #
    # python qr.py "select * from pessoas.csv where nome like '%Rosa%'"
    #
    # python qr.py "select sexo, count(*), max(idade) from pessoas.csv GROUP BY sexo order by 2 DESC"
    #
    # python qr.py "SELECT id_customer, count(*), sum(valor) FROM fat3.csv WHERE datahora_fatura.month=1 GROUP BY id_customer ORDER BY 2 desc" -d "|" -e utf-8 -f "y-m-d"
    # ------------------------------------------------------
