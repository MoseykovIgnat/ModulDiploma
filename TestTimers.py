import time

a = 0


class Every5sec():
    def __init__(self):
        self.quant_amount = 1

    def deq(self):
        self.quant_amount -= 1

    def quant_setter(self, new_amount_quant):
        self.quant_amount = new_amount_quant

    def message(self):
        print("Прошло 5 секунд")

    def getQuant(self):
        return self.quant_amount


class Every10sec:
    def __init__(self):
        self.quant_amount = 2

    def deq(self):
        self.quant_amount -= 1

    def quant_setter(self, new_amount_quant):
        self.quant_amount = new_amount_quant

    def message(self):
        print("Прошло 10 секунд")

    def getQuant(self):
        return self.quant_amount


class Every60sec:
    def __init__(self):
        self.quant_amount = 20

    def deq(self):
        self.quant_amount -= 1

    def quant_setter(self, new_amount_quant):
        self.quant_amount = new_amount_quant

    def message(self):
        print("Прошла 1 минута")

    def getQuant(self):
        return self.quant_amount


a5 = Every5sec()
a10 = Every10sec()
a60 = Every60sec()
mytime = 0
while True:
    curtime = time.time()
    if (a5.quant_amount == 0):
        a5.message()
        a5.quant_setter(1)
        mytime+=5
        print("Прошло секунд всего: "+str(mytime))
    if (a10.quant_amount == 0):
        a10.message()
        a10.quant_setter(2)
    if (a60.quant_amount == 0):
        a60.message()
        a60.quant_setter(12)

    a5.deq()
    a10.deq()
    a60.deq()
    time.sleep(5 - ((time.time() - curtime) % 60.0))