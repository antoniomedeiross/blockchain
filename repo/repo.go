package repo

import (
	"pbl-2/models"
	"pbl-2/ricart"
	"sync"
)

// PEARS -------------------------------------------------------------
var Mutex sync.RWMutex

var Peers = make(map[string] models.Peer)

// ALGORITMO RICART
var RicartInstance *ricart.Ricart

// DRONES ------------------------------------------------------------
var DroneMutex sync.RWMutex
var Drones = make(map[string]models.Drone)

func AtualizarDrone(d models.Drone) {
    DroneMutex.Lock()
    defer DroneMutex.Unlock()
    Drones[d.ID] = d
}

func BuscarDrones() map[string]models.Drone {
    DroneMutex.RLock()
    defer DroneMutex.RUnlock()
    copia := make(map[string]models.Drone)
    for k, v := range Drones {
        copia[k] = v
    }
    return copia
}