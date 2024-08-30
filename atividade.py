import sqlite3
import time
import psutil
import os
import matplotlib.pyplot as plt
import pandas as pd

class BTreeNode:
    def __init__(self, t):
        self.t = t  
        self.keys = []
        self.children = []
        self.leaf = True

    def split(self, parent, payload):
        new_node = self.__class__(self.t)
        mid_point = self.size // 2
        split_value = self.keys[mid_point]
        parent.add_key(split_value)

        new_node.children = self.children[mid_point + 1:]
        self.children = self.children[:mid_point + 1]
        new_node.keys = self.keys[mid_point + 1:]
        self.keys = self.keys[:mid_point]

        if len(new_node.children) > 0:
            new_node.leaf = False

        parent.children = parent.add_child(new_node)

    @property
    def is_full(self):
        return self.size == 2 * self.t - 1

    @property
    def size(self):
        return len(self.keys)

    def add_key(self, value):
        self.keys.append(value)
        self.keys.sort()

    def add_child(self, new_node):
        i = len(self.children) - 1
        while i >= 0 and self.children[i].keys[0] > new_node.keys[0]:
            i -= 1
        return self.children[:i + 1] + [new_node] + self.children[i + 1:]

class BTree:
    def __init__(self, t):
        self.t = t
        self.root = BTreeNode(t)

    def insert(self, payload):
        node = self.root
        if node.is_full:
            new_root = BTreeNode(self.t)
            new_root.children.append(self.root)
            new_root.leaf = False
            node.split(new_root, payload)
            self.root = new_root
        self._insert_non_full(self.root, payload)

    def _insert_non_full(self, node, payload):
        if node.leaf:
            node.add_key(payload)
        else:
            i = node.size - 1
            while i >= 0 and payload < node.keys[i]:
                i -= 1
            i += 1
            child = node.children[i]
            if child.is_full:
                node.split(node, payload)
                if payload > node.keys[i]:
                    i += 1
            self._insert_non_full(node.children[i], payload)

    def search(self, key, node=None):
        if node is None:
            node = self.root
        i = 0
        while i < node.size and key > node.keys[i]:
            i += 1
        if i < node.size and key == node.keys[i]:
            return node
        elif node.leaf:
            return None
        else:
            return self.search(key, node.children[i])

    def delete(self, key, node=None):
        # Deleção da Árvore B (não implementado)
        pass

    def __str__(self):
        return self._print_tree(self.root, "", True)

    def _print_tree(self, node, indent, last):
        ret = indent
        if last:
            ret += "└─"
            indent += "  "
        else:
            ret += "├─"
            indent += "| "

        ret += str(node.keys) + "\n"

        for i, child in enumerate(node.children):
            ret += self._print_tree(child, indent, i == len(node.children) - 1)
        return ret

class SimpleDatabase:
    def __init__(self, t):
        self.tree = BTree(t)
        self.conn = sqlite3.connect('database.db')
        self.cursor = self.conn.cursor()
        self._create_table()
        self.performance_data = []  # Armazena dados de desempenho

    def _create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS KeyValue (
                key INTEGER PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.commit()

    def create(self, key, value):
        start_time = time.time()
        if self.read(key):
            return False
        self.tree.insert(key)
        self.cursor.execute('INSERT INTO KeyValue (key, value) VALUES (?, ?)', (key, value))
        self.conn.commit()
        end_time = time.time()
        exec_time = end_time - start_time
        self.performance_data.append(('Create', key, exec_time, self.memory_usage()))
        return True

    def read(self, key):
        start_time = time.time()
        self.cursor.execute('SELECT value FROM KeyValue WHERE key = ?', (key,))
        result = self.cursor.fetchone()
        end_time = time.time()
        exec_time = end_time - start_time
        self.performance_data.append(('Read', key, exec_time, self.memory_usage()))
        return result[0] if result else None

    def update(self, key, value):
        start_time = time.time()
        if not self.read(key):
            return False
        self.cursor.execute('UPDATE KeyValue SET value = ? WHERE key = ?', (value, key))
        self.conn.commit()
        end_time = time.time()
        exec_time = end_time - start_time
        self.performance_data.append(('Update', key, exec_time, self.memory_usage()))
        return True

    def delete(self, key):
        start_time = time.time()
        if not self.read(key):
            return False
        self.cursor.execute('DELETE FROM KeyValue WHERE key = ?', (key,))
        self.conn.commit()
        end_time = time.time()
        exec_time = end_time - start_time
        self.performance_data.append(('Delete', key, exec_time, self.memory_usage()))
        return True

    def memory_usage(self):
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / (1024 ** 2)  # Em MB

    def plot_performance(self):
        df = pd.DataFrame(self.performance_data, columns=['Operation', 'Key', 'Time (s)', 'Memory (MB)'])
        print(df)  # Exibe a tabela com os dados

        # Gráfico de Tempo de Execução
        plt.figure(figsize=(12, 6))
        for op in df['Operation'].unique():
            subset = df[df['Operation'] == op]
            plt.plot(subset['Key'], subset['Time (s)'], marker='o', label=op)
        plt.title('Tempo de Execução por Operação')
        plt.xlabel('Chave')
        plt.ylabel('Tempo (s)')
        plt.legend()
        plt.grid(True)
        plt.show()

        # Gráfico de Consumo de Memória
        plt.figure(figsize=(12, 6))
        for op in df['Operation'].unique():
            subset = df[df['Operation'] == op]
            plt.plot(subset['Key'], subset['Memory (MB)'], marker='o', label=op)
        plt.title('Consumo de Memória por Operação')
        plt.xlabel('Chave')
        plt.ylabel('Memória (MB)')
        plt.legend()
        plt.grid(True)
        plt.show()

if __name__ == "__main__":
    db = SimpleDatabase(2)

    # Avaliação do desempenho
    def performance_test(db):
        # Inserção
        db.create(10, "Valor 10")
        db.create(20, "Valor 20")
        db.create(5, "Valor 5")
        db.create(6, "Valor 6")

        # Leitura
        db.read(10)

        # Atualização
        db.update(10, "Novo Valor 10")

        # Remoção
        db.delete(10)

        # Mostrar os gráficos de desempenho
        db.plot_performance()

    performance_test(db)
