import sqlite3
import time
import psutil
import os
import csv

class BTreeNode:
    def __init__(self, t):
        self.t = t  
        self.keys = []
        self.children = []
        self.leaf = True

    def split(self, parent, payload):
        new_node = BTreeNode(self.t)
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
        self.conn = sqlite3.connect('database.db')  # Cria ou conecta ao arquivo database.db
        self.cursor = self.conn.cursor()
        self._create_table()

        # Inicializa o CSV
        with open('log_operations.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Operação", "Chave", "Valor", "Tempo de Execução (segundos)", "Status"])

    def _create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS KeyValue (
                key INTEGER PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.commit()

    def log_operation(self, operation, key, value, execution_time, status):
        # Loga no terminal
        if operation == "CREATE":
            if status == "Success":
                print(f"Chave {key} criada com sucesso. Tempo de execução: {execution_time} segundos.")
            else:
                print(f"Chave {key} já existe. Tempo de execução: {execution_time} segundos.")
        elif operation == "READ":
            if status == "Success":
                print(f"Chave {key} encontrada. Valor: {value}. Tempo de execução: {execution_time} segundos.")
            else:
                print(f"Chave {key} não encontrada. Tempo de execução: {execution_time} segundos.")
        elif operation == "UPDATE":
            if status == "Success":
                print(f"Chave {key} atualizada com sucesso. Tempo de execução: {execution_time} segundos.")
            else:
                print(f"Chave {key} não encontrada. Tempo de execução: {execution_time} segundos.")
        elif operation == "DELETE":
            if status == "Success":
                print(f"Chave {key} removida com sucesso. Tempo de execução: {execution_time} segundos.")
            else:
                print(f"Chave {key} não encontrada. Tempo de execução: {execution_time} segundos.")

        # Salva no CSV
        with open('log_operations.csv', mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([operation, key, value, execution_time, status])

    def create(self, key, value):
        start_time = time.time()
        if self.read(key):
            execution_time = time.time() - start_time
            self.log_operation("CREATE", key, value, execution_time, "Failed")
            return False

        self.tree.insert(key)
        self.cursor.execute('INSERT INTO KeyValue (key, value) VALUES (?, ?)', (key, value))
        self.conn.commit()
        
        execution_time = time.time() - start_time
        self.log_operation("CREATE", key, value, execution_time, "Success")
        return True

    def read(self, key):
        start_time = time.time()
        self.cursor.execute('SELECT value FROM KeyValue WHERE key = ?', (key,))
        result = self.cursor.fetchone()
        execution_time = time.time() - start_time

        if result:
            self.log_operation("READ", key, result[0], execution_time, "Success")
            return result[0]
        else:
            self.log_operation("READ", key, None, execution_time, "Failed")
            return None

    def update(self, key, value):
        start_time = time.time()
        if not self.read(key):
            execution_time = time.time() - start_time
            self.log_operation("UPDATE", key, value, execution_time, "Failed")
            return False

        self.cursor.execute('UPDATE KeyValue SET value = ? WHERE key = ?', (value, key))
        self.conn.commit()
        
        execution_time = time.time() - start_time
        self.log_operation("UPDATE", key, value, execution_time, "Success")
        return True

    def delete(self, key):
        start_time = time.time()
        if not self.read(key):
            execution_time = time.time() - start_time
            self.log_operation("DELETE", key, None, execution_time, "Failed")
            return False

        self.cursor.execute('DELETE FROM KeyValue WHERE key = ?', (key,))
        self.conn.commit()
        
        execution_time = time.time() - start_time
        self.log_operation("DELETE", key, None, execution_time, "Success")
        return True

    def memory_usage(self):
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / (1024 ** 2)  # Retorna o uso de memória em MB

    def __str__(self):
        self.cursor.execute('SELECT * FROM KeyValue')
        rows = self.cursor.fetchall()
        return f"Árvore B:\n{self.tree}\nDados no SQLite:\n{rows}"

if __name__ == "__main__":
    db = SimpleDatabase(2)

    # Avaliação do desempenho
    def performance_test(db):
        print(f"Uso de memória antes das operações: {db.memory_usage()} MB")

        # Inserção
        db.create(10, "Valor 10")
        db.create(20, "Valor 20")
        db.create(5, "Valor 5")
        db.create(6, "Valor 6")

        # Leitura
        db.read(10)

        # Atualização
        db.update(10, "Novo Valor 10")
        db.read(10)

        # Remoção
        db.delete(10)
        
        print(db)

        print(f"Uso de memória após as operações: {db.memory_usage()} MB")

    performance_test(db)
