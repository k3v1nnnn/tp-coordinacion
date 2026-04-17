from common import fruit_item

class FruitCounter:
    def __init__(self):
        self.counter = {}
    
    def add(self, client_id, fruit_name, amount):
        client = self.counter.get(client_id, None)
        if client == None:
            self.counter[client_id] = client = {}
        fruit = client.get(fruit_name, fruit_item.FruitItem(fruit_name, 0))
        self.counter[client_id][fruit_name] = fruit + fruit_item.FruitItem(fruit_name, int(amount))
    
    def get(self, client_id):
        fruits = self.counter.pop(client_id, None)
        totals = []
        if fruits == None:
            return totals
        for fruit in fruits.values():
            totals.append([client_id, fruit.fruit, fruit.amount])
        return totals

        

