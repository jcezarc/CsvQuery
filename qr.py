import sys
import csv


class CsvQuery:

    def __init__(self, params):
        KEYWORDS = {
            'SELECT': self.parse_fields,
            'FROM': self.parse_tablename,
            'WHERE': self.parse_condition,
            'LIKE': self.parse_like,
            'GROUP': self.parse_group,
            'ORDER': self.parse_sort,
            'LIMIT': self.parse_limit,
        }
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
        param_list = params.split(' ')
        for param in param_list:
            new_function = KEYWORDS.get(param.upper())
            if new_function:
                self.parse_function = new_function
            elif self.parse_function:
                self.parse_function(param)
            else:
                raise Exception(f'Unknown "{param}" ..!!')

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
        self.field_list.setdefault(field, []).append(func_type)

    def parse_tablename(self, param):
        self.reader = csv.DictReader(
            open(param, 'r'),
            delimiter=','
        )
        if self.all_fields:
            names = self.reader.fieldnames
            self.field_list = {f: '' for f in names}

    def parse_condition(self, param):
        value = param
        if param in ['AND', 'OR', 'and', 'or']:
            value = param.lower()
        elif param == '=':
            value = '=='
        elif param.isidentifier():
            self.conditions['fields'].append(param)
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
        if not param:
            return
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
            value = try_numeric(row[field])
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
                        try_numeric(value)
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
        for field in self.field_list:
            size = self.size_of[field]
            head += truncate(field, size)
            line += '-' * (size-3) + '-+-'
        print(head)
        print(line)
        for row in dataset:
            line = ''
            for field in self.field_list:
                size = self.size_of[field]
                line += truncate(
                    str(row[field]),
                    size-3
                ) + ' | '
            print(line)


def try_numeric(s):
    num = s.split('.')
    if num[0].isnumeric():
        if len(num) == 2:
            return float(s)
        else:
            return int(s)
    return s        


if __name__ == '__main__':
    if len(sys.argv) == 2:
        query = CsvQuery(sys.argv[1])
        query.run()
    else:
        print('''
        '* * *  QR 2.0  * * * '

        How to use:
            > python qr.py "<query SQL to CSV file>"
        ''')
    # ------------- Exemplos: ------------------------------
    # python qr.py "SELECT nome, idade FROM pessoas.csv WHERE idade < 35 AND sexo = 'F' ORDER BY nome LIMIT 20"
    #
    # python qr.py "select * from pessoas.csv where nome like '%Rosa%'"
    #
    # python qr.py "select sexo, count(*), max(idade) from pessoas.csv GROUP BY sexo order by 2 DESC"
    # ------------------------------------------------------
