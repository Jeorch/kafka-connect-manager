package utils

func RemoveSliceItem(originSlice []string, item string) []string {
	switch len(originSlice) {
	case 0:
		return nil
	case 1:
		if originSlice[0] == item {
			return nil
		}
	default:
		for i, _ := range originSlice[:] {
			if originSlice[i] == item {
				originSlice = append(originSlice[:i], originSlice[i+1:]...)
				return originSlice
			}
		}
	}

	return originSlice
}

//TODO:优化算法
//func RemoveSliceItems(originSlice []string, items ...string) []string {
//	for _, item := range items {
//		for i, _ := range originSlice {
//			if originSlice[i] == item {
//				originSlice = append(originSlice[:i], originSlice[i+1:]...)
//			}
//		}
//	}
//	return originSlice
//}