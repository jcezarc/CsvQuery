import os
import sys
import csv
from datetime import datetime as dt

QR_VERSION = '1.2021.03.14 r 20.58'

class CsvQuery:

    def __init__(self, command, delimiter, encoding, date_format):
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
        self.words = command.split(' ')        
        self.content = ''
        self.is_subquery = False
        self.finished = False
        self.parse()

    def set_words(self, words):
        self.words = words
        self.is_subquery = True

    def parse(self):
        KEYWORDS = {
            'SELECT': self.get_fields,
            'FROM': self.get_tablename,
            'WHERE': self.get_condition,
            'LIKE': self.get_like_expr,
            'IN': self.get_sub_query,
            'GROUP': self.get_group,
            'ORDER': self.get_sort_field,
            'LIMIT': self.get_limit,
        }
        parenthesis_count = 0
        while self.words:
            word = self.words.pop(0)
            if not word:
                continue
            if self.is_subquery:
                if '(' in word:
                    parenthesis_count +=1
                if  ')' in word:
                    parenthesis_count -= 1
                if not parenthesis_count: break
            new_function = KEYWORDS.get(word.upper())
            if new_function:
                self.parse_function = new_function
            elif self.parse_function:
                self.parse_function(word)
            else:
                self.content += word
        self.finished = True

    def adjust_format(self):
        if '%' in self.date_format:
            return
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

    def get_fields(self, param):
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

    def get_tablename(self, param):
        filename, file_extension = os.path.splitext(param)
        if not file_extension:
            param += '.csv'
        self.reader = csv.DictReader(
            open(param, 'r', 
                encoding=self.encoding
            ),delimiter=self.delimiter
        )
        if self.all_fields:
            names = self.reader.fieldnames
            self.field_list = {f: [''] for f in names}

    def get_condition(self, param):
        COMPARE_SYMBOLS = {
            '=': '==',
            '<>': '!='
        }
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
        if param in ['AND', 'OR', 'and', 'or']:
            value = param.lower()
        elif param.split('(')[0].upper() == 'IN':
            self.parse_function = self.get_sub_query
        elif param in COMPARE_SYMBOLS:
            value = COMPARE_SYMBOLS[param]
        elif param.isidentifier():
            self.conditions['fields'].append(param)
        elif '.' in param:
            self.conditions['fields'].append(
                param.split('.')[0]
            )
        if self.conditions['values']:
            value = ' ' + value
        self.conditions['values'] += value

    def get_sub_query(self, param):
        query = CsvQuery(
            command=param,
            delimiter=self.delimiter,
            encoding=self.encoding,
            date_format=self.date_format
        )
        query.set_words(self.words)
        if not query.finished:
            query.parse()
        query.set_content()
        self.conditions['values'] = '{} in {}'.format(
            self.conditions['fields'][-1],
            query.content
        )
        self.parse_function = self.get_condition

    def get_like_expr(self, param):
        words = self.conditions['values'].split(' ')
        removing = True
        fields = self.conditions['fields']
        while removing:
            if words.pop(-1).upper() == 'NOT':
                param += ' not'
                self.conditions['fields'] = fields[:-1]
            else:
                removing = False
        self.conditions['values'] = '{} {} in {}'.format(
            ' '.join(words),
            param.replace('%', ''),
            self.conditions['fields'][-1]
        )
        self.parse_function = self.get_condition

    def get_group(self, param):
        param = param.replace(' ', '')
        if param.upper() == 'BY':
            self.limit = 0
            return
        self.group_field = param

    def get_sort_field(self, param):
        word = param.upper()
        if word == 'BY':
            return
        if self.sort_by and word == 'DESC':
            self.reverse_sorting = True
            return
        self.sort_by = param

    def get_limit(self, param):
        self.limit = self.try_numeric(param.replace(')', ''))

    def filtered_row(self, row):
        if not self.conditions['values']:
            return True
        for field in self.conditions['fields']:
            if not field: 
                continue
            value = self.try_numeric(row[field])
            if isinstance(value, str):
                if '.' in self.conditions['values']:
                    return False  # -- Expected Type = DATE
                func = self.field_list.get(field, [''])[-1]
                if self.group_field and func != 'count':
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
                if not field: 
                    continue
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
                i = min(
                    int(self.sort_by),
                    len(self.field_list)
                )
                self.sort_by = list(self.field_list)[i-1]
            result = sorted(result, key=lambda k: k[self.sort_by])
            if self.reverse_sorting: result = result[::-1]
        if group and self.limit:
            result = result[:self.limit]
        return result

    def set_content(self):
        if self.content or not self.field_list:
            return
        field = list(self.field_list)[0]
        result = []
        for row in self.scan():
            resut.append(row[field])
        self.content = result

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
                if not field:
                    continue
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


def extract_args(options, default):
    result = {v[0]: v[1] for v in options.values()}
    key = ''
    arg_list = [a for a in sys.argv if a != sys.argv[0]]
    for arg in arg_list:
        if arg in options:
            key = options[arg][0]
        elif key:
            if callable(result[key]):
                func = result[key]
                result[key] = func(arg)
            else:
                result[key] = arg
            key = ''
        else:
            result[default] = arg
    return result

def load_file(path):
    with open(path, 'r') as f:
        text = f.read()
        f.close()
    return text.replace('\n', ' ').replace('\t', ' ')

if __name__ == '__main__':
    options = {
        '-l': ('command', load_file),
        '-d': ('delimiter', ','),
        '-e': ('encoding', None),
        '-f': ('date_format', 'y-m-d'),
    }
    if len(sys.argv) > 1:
        params = extract_args(options, default='command')
        query = CsvQuery(**params)
        query.run()
    else:
        display = lambda v: v[1].__name__ if callable(v[1]) else v[0]
        print('''
        '* * *  QR {}  * * * '

        How to use:
            > python qr.py "<command>" [options]
            options: {}
        '''.format(
            QR_VERSION,
            ''.join(f'\n\t\t{k} <{display(v)}>' for k, v in options.items())
        ))
    # ------------- Exemplos: ------------------------------
    # python qr.py "SELECT nome, idade FROM pessoas.csv WHERE idade < 35 AND sexo = 'F' ORDER BY nome LIMIT 20"
    #
    # python qr.py "select * from pessoas.csv where nome like '%Rosa%'"
    #
    # python qr.py "select sexo, count(*), max(idade) from pessoas.csv GROUP BY sexo order by 2 DESC"
    #
    # python qr.py "SELECT id_customer, count(*), sum(valor) FROM fat3.csv WHERE datahora_fatura.month=1 GROUP BY id_customer ORDER BY 2 desc" -d "|" -e utf-8 -f "y-m-d"
    #
    # python qr.py "select title, movieId from movies where movieId in (527,457,362,333,260,231,163,157,151,101,50,47) LIMIT 15" -e utf-8
    #
    # ------------------------------------------------------
