package server

// Port from mysql, see sql_acl.cc for more information
type ACL struct {
	clientCapabilities uint32
	user               User
	databaseName       string
	userPass           bool
	host               string
}

// Initialize structures responsible for user/db-level privilege checking and
// load privilege information for them from tables in the 'mysql' database.
//
//  SYNOPSIS
//    acl_init()
//      dont_read_acl_tables  TRUE if we want to skip loading data from
//                            privilege tables and disable privilege checking.
//
//  NOTES
//    This function is mostly responsible for preparatory steps, main work
//    on initialization and grants loading is done in acl_reload().
//
//  RETURN VALUES
//    0	ok
//    1	Could not initialize grant's
func (acl *ACL) aclInit(dont_read_acl_tables bool) bool {
	// Placeholder to mark to do later.
	return false
}

func (acl *ACL) reuse() {
	acl.clientCapabilities = 0
	acl.user.userName = ""
	acl.user.userSalt = acl.user.userSalt[:0]
	acl.databaseName = ""
	acl.userPass = false
	acl.host = ""
}
