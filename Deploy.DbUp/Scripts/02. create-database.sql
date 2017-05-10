CREATE DATABASE [*database_name*] 
GO
ALTER DATABASE [*database_name*]  MODIFY FILE 
( NAME = N'*database_name*', MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
GO
ALTER DATABASE [*database_name*]  MODIFY FILE 
( NAME = N'*database_name*' , MAXSIZE = 2048GB , FILEGROWTH = 10%)
GO