import sys
import random
import string

Amount = 1024 * 1024
MaxKeySize = 30
MaxValueSize = 200

i = 0
fd = open("data.dat", "wb+")
for i in range(Amount):
  key = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(1, 30)))
  fd.write(len(key).to_bytes(4, sys.byteorder))
  fd.write(key.encode("ascii"))
  value = ''.join(random.choice(string.ascii_letters + string.digits) for _ in
          range(random.randint(1, 200)))
  fd.write(len(value).to_bytes(4, sys.byteorder))
  fd.write(value.encode("ascii"))
  print(i, Amount)
fd.close()
