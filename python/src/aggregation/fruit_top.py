from common import fruit_item
import bisect

class FruitTop:
    def __init__(self):
        self.top = {}

    
    def _add_sorted(self, client_id, fruit, amount):
        bisect.insort(self.top[client_id], fruit_item.FruitItem(fruit, amount))

    def add(self, client_id, fruit_name, amount):
        top = self.top.get(client_id, None)
        if top == None:
            self.top[client_id] = top = []
        for i in range(len(top)):
            if top[i].fruit == fruit_name:
                self.top[client_id][i] = self.top[client_id][i] + fruit_item.FruitItem(fruit_name, amount)
                return
        self._add_sorted(client_id, fruit_name, amount)
    
    def get(self, client_id):
        top = self.top.pop(client_id, None)
        if top == None:
            return []
        top = list(top)
        top.reverse()
        return list(
            map(
                lambda _fruit_item: (_fruit_item.fruit, _fruit_item.amount),
                top,
            )
        )