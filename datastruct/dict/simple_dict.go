package dict

import "gokv/interface/datastruct"

type SimpleDict struct {
	m map[string]any
}

func NewSimpleDict() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]any),
	}
}

func (s *SimpleDict) Get(key string) (val any, exists bool) {
	if s == nil {
		panic("SimpleDict is nil")
	}
	val, exists = s.m[key]
	return
}

func (s *SimpleDict) Len() int32 {
	if s == nil {
		panic("SimpleDict is nil")
	}
	return int32(len(s.m))
}

func (s *SimpleDict) Put(key string, val any) (result int32) {
	if s == nil {
		panic("SimpleDict is nil")
	}
	s.m[key] = val
	return 1
}

func (s *SimpleDict) PutIfAbsent(key string, val any) (result int32) {
	if s == nil {
		panic("SimpleDict is nil")
	}
	_, exists := s.m[key]
	if exists {
		return
	}
	s.m[key] = val
	return 1
}

func (s *SimpleDict) PutIfExists(key string, val any) (result int32) {
	if s == nil {
		panic("SimpleDict is nil")
	}
	_, exists := s.m[key]
	if !exists {
		return
	}
	s.m[key] = val
	return 1
}

func (s *SimpleDict) Remove(key string) (result int32) {
	if s == nil {
		panic("SimpleDict is nil")
	}
	_, exists := s.m[key]
	if !exists {
		return
	}
	delete(s.m, key)
	return 1
}

func (s *SimpleDict) ForEach(consumer datastruct.Consumer) {
	if s == nil {
		panic("SimpleDict is nil")
	}
	for key, val := range s.m {
		if continues := consumer(key, val); !continues {
			return
		}
	}
}

func (s *SimpleDict) Keys() []string {
	if s == nil {
		panic("SimpleDict is nil")
	}
	result := make([]string, s.Len())
	i := 0
	for key := range s.m {
		result[i] = key
	}
	return result
}

func (s *SimpleDict) RandomKeys(limit int32) []string {
	if s == nil {
		panic("SimpleDict is nil")
	}
	result := make([]string, limit)
	for i := 0; i < int(limit); i++ {
		for key := range s.m {
			result[i] = key
			break
		}
	}
	return result
}

func (s *SimpleDict) RandomDistinctKeys(limit int32) []string {
	if limit >= s.Len() {
		return s.Keys()
	}
	result := make([]string, limit)
	i := 0
	for key := range s.m {
		result[i] = key
		i++
		if i == int(limit) {
			break
		}
	}
	return result
}

func (s *SimpleDict) Clear() {
	if s == nil {
		panic("SimpleDict is nil")
	}
	*s = *NewSimpleDict()
}
