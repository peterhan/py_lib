def fmap_element(f, el):
    return f(el)

def fmap_list(f, l):
    return [fmap_element(f, el) for el in l)]

def fmap_lol(f, lol):
    return [fmap_list(f,l) for l in lol]

def split_nt_lol(nt_lol):
    return dict((name, fmap_lol(lambda nt: getattr(nt, name), nt_lol)) 
                for name in nt_lol[0][0]._fields)
                

combo_mat = [[combo(i + 3*j, float(i + 3*j), str(i + 3*j)) for i in range(3)]
                 for j in range(3)]
split_nt_lol(combo_mat)