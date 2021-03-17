import os
import sys
import csv
from datetime import datetime as dt

QR_VERSION = '0.2021.03.17 r 15.44'

class CsvQuery:

    AGG_FUNCS = {
        'max': max,
        'min': min,
        'sum': sum,
        'count': len,
        'avg': lambda x: sum(x) / len(x)
    }

    def __init__(self, command, delimiter, encoding, date_format):
        self.delimiter = delimiter
        self.encoding = encoding
        self.date_format = date_format
        self.adjust_format()
        self.reader = None
        self.field_list = {}
        self.conditions = {
            'fields': [],
            'expr': '',
        }
        self.sort_by = ''
        self.group_field = ''
        self.limit = 10
        self.size_of = {}
        self.parse_function = None
        self.reverse_sorting = False
        self.all_fields = True
        self.func_type = ''
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
        for word in self.mult_split(command):
            new_function = KEYWORDS.get(word.upper())
            if new_function:
                self.parse_function = new_function
            elif self.parse_function:
                self.parse_function(word)
            else:
                raise Exception(f'Unknown {word}')

    @staticmethod
    def mult_split(s, sep_out='\n\t ,*()',sep_in="><=", sublist_id='IN'):
        word, result = '', []
        separators = sep_out+sep_in
        quotes = False
        brackets = 0
        is_sub_list = False
        for c in s:
            if c not in separators or quotes:
                if c == "'":
                    quotes = not quotes
                word += c
            else:
                if c == '(':
                    brackets += 1
                    if brackets == 1 and is_sub_list: 
                        continue
                elif c == ')':
                    brackets -= 1
                if brackets and is_sub_list:
                    word += c
                else:
                    if word:
                        if word.upper() == sublist_id:
                            is_sub_list = True
                        result.append(word)
                        word = ''
                    if c in sep_in:
                        result.append(c)
        if word: result.append(word)
        return result

    def adjust_format(self):
        ELEMENT_MASK = {
            'd': '%d',
            'm': '%m',
            'y': '%Y'
        }
        if '%' in self.date_format:
            return
        for separator in ['/', '-']:
            elements = self.date_format.lower().split(separator)
            if len(elements) < 3:
                continue
            self.date_format = separator.join([
                ELEMENT_MASK[e] for e in elements
            ])
            return

    def get_fields(self, param):
        self.all_fields = False
        if param in self.AGG_FUNCS:
            self.func_type = param
        elif param.isidentifier():
            self.field_list.setdefault(
                param, []
            ).append(
                self.func_type.lower()
            )
            self.func_type = ''

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
        value = param
        if param in ['AND', 'OR', 'and', 'or', 'NOT', 'not']:
            value = param.lower()
        elif param in COMPARE_SYMBOLS:
            value = COMPARE_SYMBOLS[param]
        elif param.isidentifier():
            self.conditions['fields'].append(param)
        elif '.' in param:
            self.conditions['fields'].append(
                param.split('.')[0]
            )
        self.conditions['expr'] += value+' '

    def get_like_expr(self, param):
        def rearrange(param, expr, fields):
            words = [c for c in expr.split(' ') if c]
            removing = True
            while removing:
                if words.pop(-1).upper() == 'NOT':
                    param += ' not'
                else:
                    removing = False
            return '{} {} in {}'.format(
                ' '.join(words),
                param.replace('%', ''),
                fields[-1]
            )
        self.conditions['expr'] = rearrange(param, **self.conditions)
        self.parse_function = self.get_condition

    def get_sub_query(self, param):
        try:
            query = CsvQuery(
                command=param,
                delimiter=self.delimiter,
                encoding=self.encoding,
                date_format=self.date_format
            )
            query.parse(True)
        except:
            query = None
        self.conditions['expr'] = '{} in {}'.format(
            self.conditions['fields'][-1],
            query.sample() if query else f'({param})'
        )
        self.parse_function = self.get_condition

    def get_group(self, param):
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
        self.limit = self.try_numeric(param)

    def filtered_row(self, row):
        if not self.conditions['expr']:
            return True
        for field in self.conditions['fields']:
            value = self.try_numeric(row[field])
            exec(f"{field} = value")
        return eval(self.conditions['expr'])

    def aggregate(self, group):
        result = []
        for key, values in group.items():
            record = {}
            for field in self.field_list:
                record[self.group_field] = key
                for func in self.field_list[field]:
                    if not func:
                        continue
                    value = values[field]
                    size = self.size_of[field]
                    if func == 'count':
                        new = func
                    else:
                        new = f'{func}_{field}'
                    self.size_of[new] = max(size, len(new)) + 5
                    record[new] = self.AGG_FUNCS[func](value)
            result.append(record)
        self.field_list = {f: [''] for f in record}
        return result

    def scan(self):
        result = []
        group = {}
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
                if len(result) == self.limit: break        
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

    def sample(self):
        field = list(self.field_list)[0]
        result = []
        for row in self.scan():
            result.append(
                self.try_numeric(row[field])
            )
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
    return text

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
    # "SELECT nome, idade FROM pessoas.csv WHERE idade < 35 AND sexo = 'F' ORDER BY nome LIMIT 20"
    #
    # "select * from pessoas.csv where nome like '%Rosa%'"
    #
    # "select sexo, count(*), max(idade) from pessoas.csv GROUP BY sexo order by 2 DESC"
    #
    # "SELECT id_customer, count(*), sum(valor) FROM fat3.csv WHERE datahora_fatura.month=1 GROUP BY id_customer ORDER BY 2 desc LIMIT 20" -d "|" -e utf-8 -f "y-m-d"
    #
    # "select title, movieId from movies where movieId in (527,457,362,333,260,231,163,157,151,101,50,47) LIMIT 15" -e utf-8
    #
    #  -e utf-8 -l best_movies.sql
    # ------------------------------------------------------
