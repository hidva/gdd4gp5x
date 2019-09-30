package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/awalterschulze/gographviz"
	_ "github.com/lib/pq"
)

var conflict_table = map[string][]string{
	"AccessShareLock": {
		"AccessExclusiveLock",
	},
	"RowShareLock": {
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
	"RowExclusiveLock": {
		"ShareLock",
		"ShareRowExclusiveLock",
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
	"ShareUpdateExclusiveLock": {
		"ShareUpdateExclusiveLock",
		"ShareLock",
		"ShareRowExclusiveLock",
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
	"ShareLock": {
		"RowExclusiveLock",
		"ShareUpdateExclusiveLock",
		"ShareRowExclusiveLock",
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
	"ShareRowExclusiveLock": {
		"ShareLock",
		"RowExclusiveLock",
		"ShareUpdateExclusiveLock",
		"ShareRowExclusiveLock",
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
	"ExclusiveLock": {
		"RowShareLock",
		"ShareLock",
		"RowExclusiveLock",
		"ShareUpdateExclusiveLock",
		"ShareRowExclusiveLock",
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
	"AccessExclusiveLock": {
		"AccessShareLock",
		"RowShareLock",
		"ShareLock",
		"RowExclusiveLock",
		"ShareUpdateExclusiveLock",
		"ShareRowExclusiveLock",
		"ExclusiveLock",
		"AccessExclusiveLock",
	},
}

var cmdopt struct {
	conn_str string
	dotfile  string
}

/* The address of edge can be used as edge id in a graph. */
type Vertex struct {
	id           int
	mppsessionid int64
	edges_in     map[*Edge]bool
	edges_out    map[*Edge]bool
}

type Edge struct {
	from, to         *Vertex
	fromlock, tolock string
	obj              *LockableObject
}

func (lock_edge *Edge) String() string {
	return fmt.Sprintf("Session %d waits for %s on %s; blocked by Session %d(granted %s);",
		lock_edge.from.mppsessionid, lock_edge.fromlock, lock_edge.obj,
		lock_edge.to.mppsessionid, lock_edge.tolock)
}

type Graph struct {
	session_map map[int64]*Vertex /* sessionid -> vertex */
}

func NewGraph() *Graph {
	return &Graph{
		session_map: make(map[int64]*Vertex),
	}
}

func (this *Graph) GetMaxSessionIdVert() *Vertex {
	maxsessid := int64(0)
	for sessionid := range this.session_map {
		if sessionid > maxsessid {
			maxsessid = sessionid
		}
	}
	return this.session_map[maxsessid]
}

/* edge always is new. */
func (this *Graph) AddEdge(edge *Edge) {
	edge.from.edges_out[edge] = true
	edge.to.edges_in[edge] = true
	return
}

func (this *Graph) Empty() bool {
	return len(this.session_map) <= 0
}

func (this *Graph) RemoveVert(vert *Vertex) {
	delete(this.session_map, vert.mppsessionid)
	for edge := range vert.edges_in {
		delete(edge.from.edges_out, edge)
	}
	for edge := range vert.edges_out {
		delete(edge.to.edges_in, edge)
	}
}

func (this *Graph) GetOrNewVertex(session int64) *Vertex {
	if vert, exists := this.session_map[session]; exists {
		return vert
	} else {
		vert = &Vertex{
			id:           len(this.session_map),
			mppsessionid: session,
			edges_in:     make(map[*Edge]bool),
			edges_out:    make(map[*Edge]bool),
		}
		this.session_map[session] = vert
		return vert
	}
}

type LockableObject struct {
	gp_segment_id sql.NullInt64
	locktype      sql.NullString
	database      sql.NullInt64
	relation      sql.NullInt64
	page          sql.NullInt64
	tuple         sql.NullInt64
	virtualxid    sql.NullString
	transactionid sql.NullInt64
	classid       sql.NullInt64
	objid         sql.NullInt64
	objsubid      sql.NullInt64
}

func (this *LockableObject) String() string {
	desc := make([]string, 0, 18)
	if this.gp_segment_id.Valid {
		desc = append(desc, fmt.Sprintf("seg:%d", this.gp_segment_id.Int64))
	}
	if this.locktype.Valid {
		desc = append(desc, fmt.Sprintf("type:%s", this.locktype.String))
	}
	if this.database.Valid {
		desc = append(desc, fmt.Sprintf("db:%d", this.database.Int64))
	}
	if this.relation.Valid {
		desc = append(desc, fmt.Sprintf("rel:%d", this.relation.Int64))
	}
	if this.page.Valid {
		desc = append(desc, fmt.Sprintf("page:%d", this.page.Int64))
	}
	if this.tuple.Valid {
		desc = append(desc, fmt.Sprintf("tuple:%d", this.tuple.Int64))
	}
	if this.virtualxid.Valid {
		desc = append(desc, fmt.Sprintf("virtualxid:%s", this.virtualxid.String))
	}
	if this.transactionid.Valid {
		desc = append(desc, fmt.Sprintf("xid:%d", this.transactionid.Int64))
	}
	if this.classid.Valid {
		desc = append(desc, fmt.Sprintf("classid:%d", this.classid.Int64))
	}
	if this.objid.Valid {
		desc = append(desc, fmt.Sprintf("objid:%d", this.objid.Int64))
	}
	if this.objsubid.Valid {
		desc = append(desc, fmt.Sprintf("objsubid:%d", this.objsubid.Int64))
	}
	return strings.Join(desc, ";")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type Lock struct {
	obj  *LockableObject
	mode string
}

func reduceGraph(lockgraph *Graph) {
	graph_changed := true
	for graph_changed {
		graph_changed = false
		for _, vert := range lockgraph.session_map {
			if len(vert.edges_in) <= 0 || len(vert.edges_out) <= 0 {
				lockgraph.RemoveVert(vert)
				graph_changed = true
			}
		}
	}
}

func main() {
	flag.StringVar(&cmdopt.conn_str, "c", "sslmode=disable", "connection string.")
	flag.StringVar(&cmdopt.dotfile, "d", "", "If not empty, will generate lock wait-for graph in the dotfile with Graphviz.")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "The %s will exit 0 when there is no deadlock in current db. "+
			"And will print the sql that can be break deadlock to the stdout, then exit 1.\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	db, err := sql.Open("postgres", cmdopt.conn_str)
	if err != nil {
		panic(err)
	}
	res, err := db.Query(`
		SELECT gp_segment_id,locktype,database,relation,page,tuple,virtualxid,transactionid
			,classid,objid,objsubid,mode,granted,mppsessionid 
		FROM pg_locks
	`) // plus `WHERE pid != pg_backend_pid()`?
	if err != nil {
		panic(err)
	}

	granted_table := make(map[LockableObject]map[string]map[int64]bool, 256)
	waiter := make(map[int64][]Lock)
	for res.Next() {
		var obj LockableObject
		var mode sql.NullString
		var mppsessionid sql.NullInt64
		var granted sql.NullBool
		if err := res.Scan(&obj.gp_segment_id, &obj.locktype, &obj.database, &obj.relation,
			&obj.page, &obj.tuple, &obj.virtualxid, &obj.transactionid, &obj.classid,
			&obj.objid, &obj.objsubid, &mode, &granted, &mppsessionid); err != nil {
			panic(err)
		}
		if !granted.Valid {
			panic(fmt.Errorf("granted is null"))
		}
		if !mode.Valid {
			panic(fmt.Errorf("mode is null"))
		}
		if !mppsessionid.Valid {
			panic(fmt.Errorf("mppsessionid is null"))
		}

		if granted.Bool {
			if _, exists := granted_table[obj]; !exists {
				granted_table[obj] = make(map[string]map[int64]bool)
			}
			if _, exists := granted_table[obj][mode.String]; !exists {
				granted_table[obj][mode.String] = make(map[int64]bool)
			}
			granted_table[obj][mode.String][mppsessionid.Int64] = true
		} else {
			waiter[mppsessionid.Int64] = append(waiter[mppsessionid.Int64], Lock{obj: &obj, mode: mode.String})
		}
	}

	lockgraph := NewGraph()
	for mppsessionid, locks := range waiter {
		for _, lock := range locks {
			conflict_modes, exists := conflict_table[lock.mode]
			if !exists {
				panic(fmt.Errorf("There isn't confilict modes for %s", lock.mode))
			}
			for _, conflict_mode := range conflict_modes {
				for tomppsess := range granted_table[*lock.obj][conflict_mode] {
					if tomppsess == mppsessionid {
						continue
					}
					lockedge := &Edge{
						from:     lockgraph.GetOrNewVertex(mppsessionid),
						to:       lockgraph.GetOrNewVertex(tomppsess),
						fromlock: lock.mode,
						tolock:   conflict_mode,
						obj:      lock.obj,
					}
					lockgraph.AddEdge(lockedge)
				}
			}
		}
	}

	reduceGraph(lockgraph)
	if lockgraph.Empty() {
		fmt.Fprintln(os.Stderr, "No deadlock")
		os.Exit(0)
	}

	fmt.Fprint(os.Stderr, "Deadlock is found: \n")
	for _, vert := range lockgraph.session_map {
		for lockedge := range vert.edges_out {
			fmt.Fprintln(os.Stderr, lockedge)
		}
	}
	if len(cmdopt.dotfile) > 0 {
		vizgraph := gographviz.NewGraph()
		graphAst, _ := gographviz.ParseString(`digraph G {}`)
		if err := gographviz.Analyse(graphAst, vizgraph); err != nil {
			panic(err)
		}
		for _, vert := range lockgraph.session_map {
			for lockedge := range vert.edges_out {
				fromidstr := strconv.Itoa(lockedge.from.id)
				toidstr := strconv.Itoa(lockedge.to.id)
				must(vizgraph.AddNode("G", fromidstr, map[string]string{"label": fmt.Sprintf(`"%d"`, lockedge.from.mppsessionid)}))
				must(vizgraph.AddNode("G", toidstr, map[string]string{"label": fmt.Sprintf(`"%d"`, lockedge.to.mppsessionid)}))
				must(vizgraph.AddEdge(fromidstr, toidstr, true, map[string]string{}))
			}
		}
		must(ioutil.WriteFile(cmdopt.dotfile, []byte(vizgraph.String()), 0644))
		fmt.Fprintf(os.Stderr, "The lock waits-for graph has been write to '%s'\n", cmdopt.dotfile)
	}

	var sessionids []string
	for !lockgraph.Empty() {
		vert := lockgraph.GetMaxSessionIdVert()
		sessionids = append(sessionids, strconv.FormatInt(vert.mppsessionid, 10))
		lockgraph.RemoveVert(vert)
		reduceGraph(lockgraph)
	}
	sessionidstr := strings.Join(sessionids, ",")
	fmt.Fprintf(os.Stderr, "You can kill these session to break deadlock. sessions: %s\n", sessionidstr)
	fmt.Printf("SELECT pg_cancel_backend(procpid) FROM pg_stat_activity WHERE sess_id IN (%s);\n", sessionidstr)
	os.Exit(1)
	return
}
