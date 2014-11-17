# coding=utf-8
# created by WangZhe on 2014/11/12

if __name__ == "__main__":
    pass

def A(a,b,c):
    print a,b,c

def B(**kwars):
    A(**kwars)

B(a=1,b=2,c=3,d=4)
