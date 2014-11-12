# coding=utf-8
# created by WangZhe on 2014/11/12

if __name__ == "__main__":
    pass

class A:
    str = 'a'
    def test(self):
        print str

temp1 = A()
temp1.test()
temp1.str = 'b'
temp2 = A()
temp2.test()

