__author__ = 'Zhe'
class A():
    def __init__(self):
        self.test()

    def test(self):
        print "a"

class B(A):
    def __init__(self):
        A.__init__(self)

    def test(self):
        print "b"

a = B()