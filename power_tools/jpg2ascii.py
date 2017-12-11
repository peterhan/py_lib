import sys

from PIL import Image



chars = "   ...',;:clodxkO0KXNWMMMM"


def pic2ascii(filename):
    output = ''
    image = Image.open(filename)
    size = getsize(image)
    image = image.resize(size)
    image = image.convert('L')
    pixs = image.load()
    for y in range(size[1]):
        for x in range(size[0]):
            output += chars[int(pixs[x, y] / 10)]
        output += '\n'
    print(output)


def getsize(image,HEIGHT = 50) :
    '''Calculate the target picture size
    '''
    s_width = image.size[0]
    s_height = image.size[1]
    t_height = HEIGHT
    t_width = (t_height*s_width)/s_height
    t_width = int(t_width * 2.3)
    t_size = (t_width, t_height)
    return t_size


if __name__ == '__main__':
    # if len(sys.argv) < 2:
        # print("Useage: pic2ascii.py filename")
        # sys.exit(1)
    # filename = sys.argv[1]
    filename="""D:\Users\\news2-hs\Pictures\windows_focus\d8c16cd865d6050db90efc106cda17f00f7b4b5b1975f8b225ee5d1fa6472598.jpg"""
    open(filename,'rb')
    pic2ascii(filename)