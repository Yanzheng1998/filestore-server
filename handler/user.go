package handler

import (
	"filestore-server/util"
	"fmt"
	"io/ioutil"
	"net/http"
	dblayer "filestore-server/db"
	"time"
)

const (
	pwd_salt = "*#890"
)

// SignupHandler : handler user registration
func SignupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		data, err := ioutil.ReadFile("./static/view/signup.html")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(data)
		return
	}
	r.ParseForm()
	username := r.Form.Get("username")
	passwd := r.Form.Get("password")

	if len(username) < 3 || len(passwd) < 5 {
		w.Write([]byte("Invalid parameter"))
		return
	}

	enc_passwrd := util.Sha1([]byte(passwd + pwd_salt))
	isSuccess := dblayer.UserSignUp(username, enc_passwrd)
	if isSuccess {
		w.Write([]byte("SUCCESS"))
	} else {
		w.Write([]byte("FAILED"))
	}
}

func SignInHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		data, err := ioutil.ReadFile("./static/view/signin.html")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(data)
		return
	}
	r.ParseForm()

	username := r.Form.Get("username")
	password := r.Form.Get("password")

	encPassword := util.Sha1([]byte(password + pwd_salt))

	pwdChecked := dblayer.UserSignin(username, encPassword)

	if !pwdChecked {
		w.Write([]byte("FAILED"))
		return
	}

	// Generate token
	token := GenToken(username)
	upRes := dblayer.UpdateToken(username, token)
	if !upRes {
		w.Write([]byte("FAILED"))
		return
	}

	// redirection
	resp := util.RespMsg{
		Code: 0,
		Msg: "Ok",
		Data: struct{
			Location string
			Username string
			Token string
		} {
			Location: "http://" + r.Host + "/static/view/home.html",
			Username: username,
			Token: token,
		},
	}

	w.Write(resp.JSONBytes())
}

func UserInfoHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Parse request
	r.ParseForm()
	username := r.Form.Get("username")
	//token := r.Form.Get("token")

	// 2. check if token is valid
	//isTokenValid := IsTokenValid(token)
	//if !isTokenValid {
	//	w.WriteHeader(http.StatusForbidden)
	//	return
	//}
	// 3. query user info
	user, err := dblayer.GetUserInfo(username)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// 4. Reponse body
	resp := util.RespMsg{
		Code: 0,
		Msg: "OK",
		Data: user,
	}
	w.Write(resp.JSONBytes())
}

func GenToken(username string) string {
	// 40 bit token md5(username + timestamp + token_salt) + timestamp[:8]
	timestamp := fmt.Sprint("%x", time.Now().Unix())
	tokenPrefix := util.MD5([]byte(username + timestamp + "_tokensalt"))
	return tokenPrefix + timestamp[:8]
}

func IsTokenValid(token string) bool {
	if len(token) != 40 {
		return false
	}
	return true
}