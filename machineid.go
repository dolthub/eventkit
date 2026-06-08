package eventkit

import "github.com/denisbrodbeck/machineid"

const InvalidMachineID = "invalid"

func MachineID(appName string) string {
	id, err := machineid.ProtectedID(appName)
	if err != nil {
		return InvalidMachineID
	}
	return id
}
