from torchmetrics import BLEUScore

translate_corpus = ['the cat is on the mat'.split(), 'the cat is on the mat'.split()]
# translate_corpus是list
# list中的每个元素是一个句子

# 有多个可供选择
# reference_corpus = [['there is a cat on the mat'.split(), 'a cat is on the mat'.split()]]
reference_corpus = [['the cat is on the mat'.split()], ['the cat is on the mat'.split()]]
# reference_corpuss是list
bleu_1 = BLEUScore(n_gram=1)  # 默认n_gram=4
bleu_2 = BLEUScore(n_gram=2)
bleu_3 = BLEUScore(n_gram=3)
bleu_4 = BLEUScore(n_gram=4)
print(bleu_1(reference_corpus, translate_corpus))
print(bleu_2(reference_corpus, translate_corpus))
print(bleu_3(reference_corpus, translate_corpus))
print(bleu_4(reference_corpus, translate_corpus))