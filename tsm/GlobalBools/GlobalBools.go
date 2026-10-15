package tsm_globalbools

import (
	"log"
	"sync"
)

/*
 *  GlobalBools is safe to use concurrently.
 *
 *  SETUP:
 *
 *      import (
 *          // "tsm/GlobalBools"
 *          "github.com/go-dev-joint/GlobalBools"
 *      )
 *      var (
 *          globalBools     tsm_globalbools.GlobalBools     // stores as said global bools with a string name as key
 *      )
 *      main(){
 *          globalBools = tsm_globalbools.GlobalBools{V: make(map[string]bool), Debug: false}
 *      }
 *
 *  USAGE:
 *      globalBools.SetGB("test", true)
 *      if globalBools.GetGB("test") { fmt.Println("test is true") } else { fmt.Println("test is false") }
 *      if !globalBools.SetGBR("test", true) { fmt.Println("was already true") } else { fmt.Println("set to true") }
 *      if globalBools.DelGB("test") { fmt.Println("deleted test") } else { fmt.Println("test already deleted") }
 *      if globalBools.DelGB("test") { fmt.Println("deleted test") } else { fmt.Println("test already deleted") }
 *
 */

type GlobalBools struct {
	V     map[string]bool
	Debug bool
	mux   sync.Mutex
}

// get all GlobalBools as copy
func (c *GlobalBools) GetAllGB() map[string]bool {
	retval := make(map[string]bool)
	c.mux.Lock()
	for name, boolv := range c.V {
		aname := string(name)
		var setv bool
		if boolv {
			setv = true
		} else {
			setv = false
		}
		retval[aname] = setv
	}
	c.mux.Unlock()
	return retval
}

// set a GlobalBools by name
func (c *GlobalBools) SetGB(name string, value bool) {
	if c.Debug {
		log.Printf("GlobalBools.SetGB name=%s value=%t", name, value)
	}
	c.mux.Lock()
	c.V[name] = value
	c.mux.Unlock()
}

// switch a GlobalBool by name and get set state
func (c *GlobalBools) SwitchGB(name string) bool {
	c.mux.Lock()
	switch c.V[name] {
	case true:
		c.V[name] = false
	case false:
		c.V[name] = true
	}
	retval := c.V[name]
	if c.Debug {
		log.Printf("GlobalBools.SwitchGB name='%s' retval=%t", name, retval)
	}
	c.mux.Unlock()
	return retval

}

// set a GlobalBool by name and return a true or false on success change or fail
func (c *GlobalBools) SetGBR(name string, value bool) bool {
	retval := false
	c.mux.Lock()
	if c.V[name] != value {
		c.V[name], retval = value, true
	}
	c.mux.Unlock()
	if c.Debug {
		log.Printf("GlobalBools.SetGBR name='%s' value=%t retval=%t", name, value, retval)
	}
	return retval
}

// get a GlobalBool by name
func (c *GlobalBools) GetGB(name string) bool {
	c.mux.Lock()
	retval := c.V[name]
	c.mux.Unlock()
	if c.Debug {
		log.Printf("GlobalBools.GetGB name='%s' retval=%t", name, retval)
	}
	return retval
}

// exists GlobalBool by name
func (c *GlobalBools) ExistsGB(name string) bool {
	//log.Printf("GlobalBools: existsGB=%s", name)
	retval := false
	c.mux.Lock()
	if _, ok := c.V[name]; ok {
		retval = true
	}
	c.mux.Unlock()
	if c.Debug {
		log.Printf("GlobalBools.ExistsGB name='%s' retval=%t", name, retval)
	}
	return retval
}

// delete a GlobalBool by name
func (c *GlobalBools) DelGB(name string) bool {
	retval := false
	c.mux.Lock()
	_, isMapContainsKey := c.V[name]
	if isMapContainsKey {
		retval = true
		delete(c.V, name)
	}

	if c.Debug {
		log.Printf("GlobalBools.DelGB name='%s' retval=%t", name, retval)
	}
	c.mux.Unlock()
	return retval
}
