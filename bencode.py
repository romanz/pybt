from collections import OrderedDict

class Error(Exception):
	pass

_encoders = {
	int:  (lambda x: 'i{}e'.format(x)),
	str:  (lambda x: '{}:{}'.format(len(x), x)),
	list: (lambda x: 'l{}e'.format( ''.join(encode(i) for i in x) )),
	OrderedDict: (lambda x: 'd{}e'.format( ''.join(encode(k) + encode(v) for k, v in x.items()) ))
}

def encode(obj):
	T = type(obj)
	enc = _encoders[T]
	return enc(obj)

def split(s, c):
	index = s.find(c)
	assert(c == s[index])
	return s[:index], s[index+1:]

def decode(s):
	obj, rest = _decode(s)
	assert rest == ''
	return obj

def _decode(s):
	if s[0] == 'i': # i<integer>s
		v, s = split(s[1:], 'e')
		res = int(v)
		return res, s


	if s[0] == 'l': # l<item1>...<itemN>e
		s = s[1:]
		res = []
		while s[0] != 'e':
			obj, s = _decode(s)
			res.append(obj)
		return res, s[1:]

	if s[0] == 'd': # d<key1><value1>...<keyN><valueN>e
		s = s[1:]
		res = OrderedDict()
		while s[0] != 'e':
			k, s = _decode(s)
			v, s = _decode(s)
			res[k] = v
		return res, s[1:]

	# length-prefixed string
	n, s = split(s, ':')
	n = int(n) 
	return s[:n], s[n:]

## Unittests
if __name__ == '__main__':
	tests = [
		(123, 'i123e'), 
		(-45, 'i-45e'), 
		('spam', '4:spam'), 
		('firefox', '7:firefox'),
		(OrderedDict([(1, 2), (3, 4)]), 'di1ei2ei3ei4ee'),
		(['spam', 'eggs', 67], 'l4:spam4:eggsi67ee'),
	]
	for x, y in tests:
		print '{} <=> {}'.format(x, y)
		assert encode(x) == y
		assert decode(y) == x
