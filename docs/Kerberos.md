[Parent](../README.md)

# Kerberos

## 组件

* DC
    * KDC
        * AS
        * TGS
    * AD
* Client
* Server

## 概念

* DC域控，也就是认证中心，（信托机构），包含KDC与AD
* KDC（Key分发中心），认证核心组件，包含AS与TGS服务
* AD（活动目录），存储Client与Server的Hash或密钥以及一些其他权限相关信息
* AS认证服务，Client首次认证获取临时票据（TGT）的服务
* TGS票据生成服务，Client获取Server访问票据（Ticket）的服务
* Client Hash：客户端首次认证加密Session Key密钥
* KDC Hash：KDC加密TGT密钥
* Server Hash：KDC加密Ticket密钥
* Session Key：Client认证后，AS服务返回的密钥，与TGT同时返回
* Server Session Key：Client经TGS认证后，TGS服务返回的密钥与Ticket同时返回
* TGT：客户端首次经AS认证后，KDC使用KDC Hash加密Client Info与Session Key返回的临时票据
* Ticket：客户端经TGS认证后，KDC使用Server Hash加密Client Info与Server Session Key返回的访问票据

## 认证过程

1. 客户端向DC发起认证请求，使用客户端密钥加密Client Info+Timestamp
    * 请求中附带Client Info
1. DC中的AS服务器，会验证Client Info是否合法，并从AD中取出相应的客户端密钥解密请求体验证
    * 验证完成后会生成Session Key
    * Session Key会经过客户端密钥加密返回
    * 此时KDC会把客户端信息+Session Key+ 到期时间，经KDC Hash加密，生成TGT返回
1. 客户端根据客户端密钥解密Session Key，然后根据Session Key加密客户端信息以及时间戳，然后再请求TGS
    * 请求中附带客户端信息，服务端信息，TGT，加密信息
1. TGS收到请求后，根据KDC Hash解密TGT，得到Session Key以及客户端信息，再根据会话密钥解密加密信息验证客户端信息
    * 验证完成后，生成服务端会话密钥（SSK），以及票据（Ticket）
    * Server Hash根据请求中的服务端信息在AD中查找
    * SSK经过会话密钥（SK）加密返回
    * 票据=根据服务端密钥（Server Hash）加密（SSK+Client Info+End Time）
1. 客户端根据SK解密SSK，然后根据SSK加密（Client Info + Timestamp）得到请求体，然后向服务端发起请求
    * 请求体：Ticket，加密信息，客户端信息
1. 服务端收到客户端请求
    * 根据服务端密钥解密票据，得到SSK
    * 根据SSK解密body，验证客户端信息，以及有效期
    * 认证结束

