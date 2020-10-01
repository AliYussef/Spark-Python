import findspark
import pyspark

findspark.init()
sc = pyspark.SparkContext.getOrCreate()

unsorted_elements = sc.textFile("/resources/jupyterlab/labs/BD0211EN/LabData/testCase1.txt", 4)


def insertionSort(unsorted_list):
    for i in range(1, len(unsorted_list)):

        key = unsorted_list[i]
        j = i - 1
        while j >= 0 and key < unsorted_list[j]:
            unsorted_list[j + 1] = unsorted_list[j]
            j -= 1
        unsorted_list[j + 1] = key

    return unsorted_list


def merge(list1, list2):
    results = []
    while list1 and list2:
        if list1[0] < list2[0]:
            results.append(list1.pop(0))
        else:
            results.append(list2.pop(0))


    results.extend(list1)
    results.extend(list2)
    return results

sorted_elements = unsorted_elements.map(lambda x: int(x)) \
    .glom().map(lambda x: insertionSort(x)).reduce(merge)

print(sorted_elements)
