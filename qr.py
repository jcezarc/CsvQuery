#!/usr/bin/python python
# -*- coding: utf-8 -*-

'''

    QR = CLI for SQL queries in CSV files!
         (no JOIN for now!)

    ------------- Examples: ------------------------------
    "SELECT nome, idade FROM pessoas.csv WHERE idade < 35 AND sexo = 'F' ORDER BY nome LIMIT 20"
    
    "select * from pessoas.csv where nome like '%Rosa%'"
    
    "select sexo, count(*), max(idade) from pessoas GROUP BY sexo order by 2 DESC"
    
    "select title, movieId from movies where movieId in (527,457,362,333,260,231,163,157,151,101,50,47) LIMIT 15" -e utf-8
    
     -e utf-8 -l best_movies.sql
    ------------------------------------------------------

'''


import os
import sys
import csv
import unicodedata
from io import open
from datetime import datetime as dt

QR_VERSION = '0.2021.06.02 r 18.10'
float_separators = ['.', ',']
date_separators = ['/', '-']


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
        self.field_functions = {}
        self.field_order = {}
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
        self.filename = ''
        self.csv_data = []
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
                raise Exception('Unknown {}'.format(word))

    def clean_text(self, text):
        return unicodedata.normalize(
            "NFKD", unicode(text)
        ).encode(
            "ASCII", "ignore"
        ) #.decode(self.encoding or 'utf-8')

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
                        word = word.strip()
                        if word.upper() == sublist_id:
                            is_sub_list = True
                        result.append(word)
                        word = ''
                    if c in sep_in:
                        if c == '>' and result[-1] == '<':
                            result[-1] = '<>'
                        else:
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
        for separator in date_separators:
            elements = self.date_format.lower().split(separator)
            if len(elements) < 3:
                continue
            self.date_format = separator.join([
                ELEMENT_MASK[e] for e in elements
            ])
            return

    @staticmethod
    def get_alias(func, field):
        if func and field:
            separator = '_'
        else:
            separator = ''
        return '{}{}{}'.format(func, separator, field)

    def get_fields(self, param):
        field = None
        if param in self.AGG_FUNCS:
            self.func_type = param
            if param == 'count': field = ''
        elif param and param[0].isalpha():
            self.all_fields = False
            field = param
        if not field is None:
            self.field_functions.setdefault(
                field, []
            ).append(
                self.func_type
            )
            index = len(self.field_functions)
            self.field_order[index] = self.get_alias(self.func_type, field)
            self.func_type = ''

    def read_csv(self, filename, encoding, delimiter):
        def get_data():
            self.csv_data = []
            for row in self.reader:
                self.csv_data.append({
                    k: self.clean_text(v) 
                    for k, v in row.items()
                })
        print('Opening {}...'.format(filename))
        for curr_encoding in set([encoding, 'utf-8', 'iso8859', 'cp850', 'ascii', None]):
            try:
                self.reader = csv.DictReader(
                    open(filename, 'r', 
                        encoding=curr_encoding
                    ),delimiter=delimiter
                )
                get_data()
                return True
            except Exception as e:
                print('\tEncoding fail: {}'.format(curr_encoding))
                print(e)
                print('-'*100)
                continue
        return False

    def get_tablename(self, param):
        self.filename, file_extension = os.path.splitext(param)
        if not file_extension:
            file_extension = '.csv'
        self.filename += file_extension
        ok = self.read_csv(self.filename, self.encoding, self.delimiter)
        if not ok:
            raise Exception('Invalid file structure in {}'.format(self.filename))
        names = [
            self.clean_text(field.replace(' ', '_'))
            for field in self.reader.fieldnames
        ]
        self.reader.fieldnames = names
        if self.all_fields:
            self.field_order = {k+1: v for k, v in enumerate(names)}
            self.field_functions = {f: [''] for f in names}

    def get_condition(self, param):
        COMPARE_SYMBOLS = {
            '=': '==',
            '<>': '!='
        }
        value = param
        if param in ['AND', 'OR', 'and', 'or', 'NOT', 'not']:
            value = ' ' +param.lower()
        elif param in COMPARE_SYMBOLS:
            value = COMPARE_SYMBOLS[param]
        elif param and param[0].isalpha():
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
        except:
            query = None
        self.conditions['expr'] = '{} in {}'.format(
            self.conditions['fields'][-1],
            query.sample() if query else '({})'.format(param)
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
            if not value:
                return False
            exec("{} = value".format(field))
        return eval(self.conditions['expr'])

    def aggregate(self, group):
        result = []
        for key, values in group.items():
            record = {}
            for field in self.field_functions:
                if self.group_field:
                    record[self.group_field] = key
                for func in self.field_functions[field]:
                    if not func:
                        continue
                    alias = self.get_alias(func, field)
                    record[alias] = self.AGG_FUNCS[func](
                        values[field]
                    )
            result.append(record)
        return result

    def scan(self):
        result = []
        group = {}
        for row in self.csv_data:
            if not self.filtered_row(row):
                continue
            record = {}
            for field in self.field_functions:
                value = row.get(field, '')
                if self.group_field:
                    key = row[self.group_field]
                    group.setdefault(
                        key, {}
                    ).setdefault(
                        field, []
                    ).append(
                        self.try_numeric(value)
                    )
                elif value:
                    curr_size = len(str(value))
                    if curr_size > self.size_of.get(field, len(field)+5):
                        curr_size += 5
                        self.size_of[field] = min(70, curr_size)
                    record[field] = value
            if not self.group_field:
                result.append(record)
                if len(result) == self.limit: break
        if group:
            result = self.aggregate(group)
        if self.sort_by:
            try:
                by_index = int(self.sort_by)
            except:
                by_index = 0
            if by_index:
                self.sort_by = self.field_order[by_index]
            result = sorted(result, key=lambda k: k[self.sort_by])
            if self.reverse_sorting: result = result[::-1]
        if self.group_field and self.limit:
            result = result[:self.limit]
        return result

    def sample(self):
        field = self.field_order[1]
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
        sub = ''
        line = ''
        count = 0
        for row in self.scan():
            line = ''
            for index in sorted(self.field_order):
                field = self.field_order[index]
                size = self.size_of.get(field, len(field)+5)
                if head:
                    head += truncate(' '+field, size)
                    sub += '-' * (size-3) + '-+-'
                line += truncate(
                    str(row[field]),
                    size-3
                ) + ' | '
            if head:
                print(head)
                print(sub)
                head = ''
            print(line)

    def try_numeric(self, expr):
        candidate = self.clean_text(expr).replace(' ', '')
        negative = candidate.startswith('-')
        is_number = True
        for c in candidate:
            if c.isalpha():
                is_number = False
                break
            if not negative and c in date_separators:
                try:
                    return dt.strptime(candidate[:10], self.date_format)
                except:
                    is_number = False
                    break
            elif c in float_separators:
                try:
                    return float(candidate.replace(',', '.'))
                except:
                    is_number = False
                    break
        if is_number:
            try:
                return int(candidate)
            except:
                pass
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
            ''.join('\n\t\t{} <{}>'.format(k, display(v))
             for k, v in options.items())
        ))
