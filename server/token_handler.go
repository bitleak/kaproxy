package server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/bitleak/kaproxy/auth"
	"github.com/bitleak/kaproxy/util"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func listToken(c *gin.Context) {
	sync, _ := strconv.ParseBool(c.Query("sync"))
	c.JSON(http.StatusOK, srv.tokenManager.ListToken(sync))
}

func getToken(c *gin.Context) {
	sync, _ := strconv.ParseBool(c.Query("sync"))
	token := srv.tokenManager.GetToken(c.Param("token"), sync)
	if token != nil {
		bytes, _ := token.Marshal()
		c.Data(http.StatusOK, "application/json; charset=utf-8", bytes)
	} else {
		c.JSON(http.StatusNotFound, auth.ErrTokenNotFound)
	}
}

func createToken(c *gin.Context) {
	var topics []string
	var groups []string

	token := c.PostForm("token")
	if token == "" {
		token = "K_" + util.GenUniqueID()
	}
	if c.PostForm("topics") != "" {
		topics = strings.Split(c.PostForm("topics"), ",")
	}
	if c.PostForm("groups") != "" {
		groups = strings.Split(c.PostForm("groups"), ",")
	}
	role := c.PostForm("role")
	err := srv.tokenManager.CreateToken(token, role, topics, groups)
	if err != nil && err == auth.ErrTokenExists {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		logger := getLogger(c)
		logger.WithFields(logrus.Fields{
			"token": token,
			"err":   err,
		}).Error("Failed to create the token")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"token": token})
}

func deteteToken(c *gin.Context) {
	token := c.Param("token")
	err := srv.tokenManager.DeleteToken(token)
	if err == auth.ErrTokenNotFound {
		c.JSON(http.StatusNotFound, auth.ErrTokenNotFound)
	} else if err != nil {
		logger := getLogger(c)
		logger.WithFields(logrus.Fields{
			"token": token,
			"err":   err,
		}).Error("Failed to delete the token")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, gin.H{"token": token})
	}
}

func updateToken(c *gin.Context) {
	var topics []string
	var groups []string

	token := c.Param("token")
	isAdd := c.Param("action") != "delete"
	if c.PostForm("topics") != "" {
		topics = strings.Split(c.PostForm("topics"), ",")
	}
	if c.PostForm("groups") != "" {
		groups = strings.Split(c.PostForm("groups"), ",")
	}
	role := c.PostForm("role")
	err := srv.tokenManager.UpdateToken(token, isAdd, role, topics, groups)
	if err != nil {
		logger := getLogger(c)
		logger.WithFields(logrus.Fields{
			"token": token,
			"err":   err,
		}).Error("Failed to update the token")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"token": token})
}
