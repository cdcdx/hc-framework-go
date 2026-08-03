// Package bcrypt 密码哈希与校验。
package bcrypt

import "golang.org/x/crypto/bcrypt"

// DefaultCost 默认 cost factor
const DefaultCost = 12

// Hash 生成 bcrypt 哈希
func Hash(password string, cost int) (string, error) {
	if cost <= 0 {
		cost = DefaultCost
	}
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// Compare 比较密码与哈希
func Compare(hashedPassword, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
}
