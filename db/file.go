package db

import (
	"database/sql"
	myDB "filestore-server/db/mysql"
	"fmt"
)

// OnFileUploadFinished : finish upload file
func OnFileUploadFinished(fileHash string, filenName string, fileSize int64, fileAddr string) bool {
	stmt, err := myDB.DBConn().Prepare(
		"insert ignore into tbl_file(`file_sha1`, `file_name`,`file_size`," +
			"`file_addr`, `status`) values (?,?,?,?,1)")
	if err != nil {
		fmt.Println("Failed to prepare statement. err: " + err.Error())
		return false
	}
	defer stmt.Close()

	ret, err := stmt.Exec(fileHash, filenName, fileSize, fileAddr)

	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	if rf, err := ret.RowsAffected(); nil == err {
		if rf <= 0 {
			fmt.Printf("File with hash:%s has been uploaded before", fileHash)
		}
		return true
	}

	return false
}

type TableFile struct {
	FileHash string
	FileName sql.NullString
	FileSize sql.NullInt64
	FileAddr sql.NullString
}

// GetFileMeta : get metadata from database
func GetFileMeta(fileHash string) (*TableFile, error) {
	stmt, err := myDB.DBConn().Prepare(
		"select file_sha1, file_addr, file_name, file_size from tbl_file" +
			" where file_sha1=? and status=1 limit 1")
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	defer stmt.Close()

	tfile := TableFile{}
	stmt.QueryRow(fileHash).Scan(&tfile.FileHash, &tfile.FileAddr, &tfile.FileName, &tfile.FileSize)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return &tfile, nil
}

// GetFileMetaList : 从mysql批量获取文件元信息
func GetFileMetaList(limit int) ([]TableFile, error) {
	stmt, err := myDB.DBConn().Prepare(
		"select file_sha1,file_addr,file_name,file_size from tbl_file " +
			"where status=1 limit ?")
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(limit)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	columns, _ := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	var tfiles []TableFile
	for i := 0; i < len(values) && rows.Next(); i++ {
		tfile := TableFile{}
		err = rows.Scan(&tfile.FileHash, &tfile.FileAddr,
			&tfile.FileName, &tfile.FileSize)
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		tfiles = append(tfiles, tfile)
	}
	fmt.Println(len(tfiles))
	return tfiles, nil
}

// UpdateFileLocation : 更新文件的存储地址(如文件被转移了)
func UpdateFileLocation(filehash string, fileaddr string) bool {
	stmt, err := myDB.DBConn().Prepare(
		"update tbl_file set`file_addr`=? where `file_sha1`=? limit 1")
	if err != nil {
		fmt.Println("fail to precompile sql, err:" + err.Error())
		return false
	}
	defer stmt.Close()

	ret, err := stmt.Exec(fileaddr, filehash)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	if rf, err := ret.RowsAffected(); nil == err {
		if rf <= 0 {
			fmt.Printf("fail to update file location, filehash:%s", filehash)
		}
		return true
	}
	return false
}
