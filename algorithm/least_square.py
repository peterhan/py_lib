##http://blog.csdn.net/u012176591/article/details/36930727
def sum_multi_2list(x,y):
    sum = 0
    for i in range(0,len(x)):
        sum += x[i]*y[i]
    return sum * 1.0
    
def least_square(x,y):
    # y = ax+b
    if len(x)!=len(y):
        print 'length should equal'
        return None,None
    sum_x = sum(x) * 1.0
    sum_y = sum(y) * 1.0
    sum_x_2 = sum(map(lambda x:x*x, x)) * 1.0
    sum_xy = sum_multi_2list(x,y)
    n=len(x)
    denominator = (n*sum_x_2-sum_x*sum_x)
    a = (n*sum_xy-sum_x*sum_y) / denominator
    b = (sum_x_2 * sum_y - sum_x * sum_xy) / denominator
    print n,sum_x,sum_y,sum_x_2,sum_xy,denominator
    return a,b
    
if __name__ == '__main__':
    x=range(15)
    y=range(1,31,2)
    print x,y
    print least_square(x,y) # y=2x+1