package eventkit

import "github.com/denisbrodbeck/machineid"

func MachineID(appName string) string {
	id, err := machineid.ProtectedID(appName)
	if err != nil {
		return "invalid"
	}
	return id
}
