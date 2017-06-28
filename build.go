// +build ignore
package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	versionRe = regexp.MustCompile(`-[0-9]{1,3}-g[0-9a-f]{5,10}`)
	goarch    string
	goos      string
	gocc      string
	gocxx     string
	cgo       string
	pkgArch   string
	version   string = "1.0.1-alph"
	// deb & rpm does not support semver so have to handle their version a little differently
	linuxPackageVersion   string = "1.0.1-alph"
	linuxPackageIteration string = ""
	race                  bool
	workingDir         string
	includeBuildNumber bool   = true
	buildNumber        int    = 0
	binaryName         string = "v-graphite"
	binarySrc          string = "./src"
	packPrefix         string = "v-graphite-linux-pack"
	packageRoot, _            = ioutil.TempDir("", packPrefix)
)

const minGoVersion = 1.8

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)

	ensureGoPath()

	flag.StringVar(&goarch, "goarch", runtime.GOARCH, "GOARCH")
	flag.StringVar(&goos, "goos", runtime.GOOS, "GOOS")
	flag.StringVar(&gocc, "cc", "", "CC")
	flag.StringVar(&gocxx, "cxx", "", "CXX")
	flag.StringVar(&cgo, "cgo-enabled", "", "CGO_ENABLED")
	flag.StringVar(&pkgArch, "pkg-arch", "", "PKG ARCH")
	flag.BoolVar(&race, "race", race, "Use race detector")
	flag.BoolVar(&includeBuildNumber, "includeBuildNumber", includeBuildNumber, "IncludeBuildNumber in package name")
	flag.IntVar(&buildNumber, "buildNumber", 0, "Build number from CI system")
	flag.Parse()

	// readVersionFromPackageJson()
	linuxPackageIteration = fmt.Sprintf("%d%s", time.Now().Unix(), linuxPackageIteration)

	log.Printf("Version: %s, Linux Version: %s, Package Iteration: %s\n", version, linuxPackageVersion, linuxPackageIteration)

	if flag.NArg() == 0 {
		log.Println("Usage: go run build.go build")
		return
	}

	workingDir, _ = os.Getwd()

	for _, cmd := range flag.Args() {
		switch cmd {
		case "setup":
			setup()

		case "build":
			clean()
			build(binaryName, binarySrc, []string{})

		case "test":
			test("./pkg/...")

		case "package":
			createLinuxPackages()

		case "pkg-rpm":
			createRpmPackages()

		case "pkg-deb":
			createDebPackages()

		case "sha-dist":
			shaFilesInDist()

		case "latest":
			makeLatestDistCopies()

		case "clean":
			clean()

		default:
			log.Fatalf("Unknown command %q", cmd)
		}
	}
}

func makeLatestDistCopies() {
	files, err := ioutil.ReadDir("dist")
	if err != nil {
		log.Fatal("failed to create latest copies. Cannot read from /dist")
	}

	latestMapping := map[string]string{
		".deb":    replaceBinaryName("dist/%s_latest_amd64.deb"),
		".rpm":    replaceBinaryName("dist/%s-latest-1.x86_64.rpm"),
		".tar.gz": replaceBinaryName("dist/%s-latest.linux-x64.tar.gz"),
	}

	for _, file := range files {
		for extension, fullName := range latestMapping {
			if strings.HasSuffix(file.Name(), extension) {
				runError("cp", path.Join("dist", file.Name()), fullName)
			}
		}
	}
}

type linuxPackageOptions struct {
	packageType            string
	homeDir                string
	binPath                string
	serverBinPath          string
	cliBinPath             string
	configDir              string
	ldapFilePath           string
	etcDefaultPath         string
	etcDefaultFilePath     string
	initdScriptFilePath    string
	systemdServiceFilePath string

	postinstSrc    string
	initdScriptSrc string
	defaultFileSrc string
	systemdFileSrc string

	depends []string
}

func createDebPackages() {
	createPackage(linuxPackageOptions{
		packageType:            "deb",
		homeDir:                replaceBinaryName("/usr/share/%s"),
		binPath:                "/usr/sbin",
		configDir:              replaceBinaryName("/etc/%s"),
		etcDefaultPath:         "/etc/default",
		etcDefaultFilePath:     replaceBinaryName("/etc/default/%s"),
		initdScriptFilePath:    replaceBinaryName("/etc/init.d/%s"),
		systemdServiceFilePath: replaceBinaryName("/usr/lib/systemd/system/%s.service"),

		postinstSrc:    "packaging/deb/control/postinst",
		initdScriptSrc: replaceBinaryName("packaging/deb/init.d/%s"),
		defaultFileSrc: replaceBinaryName("packaging/deb/default/%s"),
		systemdFileSrc: replaceBinaryName("packaging/deb/systemd/%s.service"),

		depends: []string{"adduser", "libfontconfig"},
	})
}

func createRpmPackages() {
	createPackage(linuxPackageOptions{
		packageType:            "rpm",
		homeDir:                replaceBinaryName("/usr/share/%s"),
		binPath:                "/usr/sbin",
		configDir:              replaceBinaryName("/etc/%s"),
		etcDefaultPath:         "/etc/sysconfig",
		etcDefaultFilePath:     replaceBinaryName("/etc/sysconfig/%s"),
		initdScriptFilePath:    replaceBinaryName("/etc/init.d/%s"),
		systemdServiceFilePath: replaceBinaryName("/usr/lib/systemd/system/%s.service"),

		postinstSrc:    "packaging/rpm/control/postinst",
		initdScriptSrc: replaceBinaryName("packaging/rpm/init.d/%s"),
		defaultFileSrc: replaceBinaryName("packaging/rpm/sysconfig/%s"),
		systemdFileSrc: replaceBinaryName("packaging/rpm/systemd/%s.service"),

		depends: []string{"/sbin/service", "fontconfig"},
	})
}

func createLinuxPackages() {
	createDebPackages()
	createRpmPackages()
}

func replaceBinaryName(str string) string {
	return fmt.Sprintf(str, binaryName)
}

func createPackage(options linuxPackageOptions) {

	dirs := []string{
		"dist",
		join(options.homeDir),
		join("/etc/init.d"),
		join(options.etcDefaultPath),
		join("/usr/lib/systemd/system"),
		join("/usr/sbin"),
	}
	mkdirs(dirs...)

	// copy binary
	cp(filepath.Join(workingDir, "bin/"+binaryName), join("/usr/sbin/"+binaryName))
	// copy init.d script
	cp(options.initdScriptSrc, join(options.initdScriptFilePath))
	// copy environment var file
	cp(options.defaultFileSrc, join(options.etcDefaultFilePath))
	// copy systemd file
	cp(options.systemdFileSrc, join(options.systemdServiceFilePath))
	// copy release files
	cpr(filepath.Join(workingDir, "conf"), join(options.configDir))

	args := []string{
		"-s", "dir",
		"--description", binaryName,
		"-C", packageRoot,
		"--vendor", binaryName,
		"--url", "https://github.com/coder-van/v-graphite",
		"--license", "\"Apache 2.0\"",
		"--maintainer", "github.com/coder-van",
		"--config-files", options.initdScriptFilePath,
		"--config-files", options.etcDefaultFilePath,
		"--config-files", options.systemdServiceFilePath,
		"--after-install", options.postinstSrc,
		"--name", binaryName,
		"--version", linuxPackageVersion,
		"-p", "./dist/",
	}

	if options.packageType == "rpm" {
		args = append(args, "--rpm-posttrans", "packaging/rpm/control/posttrans")
	}

	if options.packageType == "deb" {
		args = append(args, "--deb-no-default-config-files")
	}

	if pkgArch != "" {
		args = append(args, "-a", pkgArch)
	}

	if linuxPackageIteration != "" {
		args = append(args, "--iteration", linuxPackageIteration)
	}

	// add dependenciesj
	for _, dep := range options.depends {
		args = append(args, "--depends", dep)
	}

	args = append(args, ".")

	fmt.Println("Creating package: ", options.packageType)
	runPrint("fpm", append([]string{"-t", options.packageType}, args...)...)
}

func verifyGitRepoIsClean() {
	rs, err := runError("git", "ls-files", "--modified")
	if err != nil {
		log.Fatalf("Failed to check if git tree was clean, %v, %v\n", string(rs), err)
		return
	}
	count := len(string(rs))
	if count > 0 {
		log.Fatalf("Git repository has modified files, aborting")
	}

	log.Println("Git repository is clean")
}

func ensureGoPath() {
	if os.Getenv("GOPATH") == "" {
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		gopath := filepath.Clean(filepath.Join(cwd, "../../../../"))
		log.Println("GOPATH is", gopath)
		os.Setenv("GOPATH", gopath)
	}
}

func setup() {
	runPrint("go", "get", "-v", "github.com/kardianos/govendor")
	runPrint("go", "install", "-v", "./src")
}

func test(pkg string) {
	setBuildEnv()
	runPrint("go", "test", "-short", "-timeout", "60s", pkg)
}

func build(binaryName, pkg string, tags []string) {
	binary := "./bin/" + binaryName
	if goos == "windows" {
		binary += ".exe"
	}

	rmr(binary, binary+".md5")
	args := []string{"build", "-ldflags", ldflags()}
	if len(tags) > 0 {
		args = append(args, "-tags", strings.Join(tags, ","))
	}
	if race {
		args = append(args, "-race")
	}

	args = append(args, "-o", binary)
	args = append(args, pkg)
	setBuildEnv()

	runPrint("go", "version")
	runPrint("go", args...)

	// Create an md5 checksum of the binary, to be included in the archive for
	// automatic upgrades.
	err := md5File(binary)
	if err != nil {
		log.Fatal(err)
	}
}

func ldflags() string {
	var b bytes.Buffer
	b.WriteString("-w")
	b.WriteString(fmt.Sprintf(" -X main.version=%s", version))
	b.WriteString(fmt.Sprintf(" -X main.commit=%s", getGitSha()))
	b.WriteString(fmt.Sprintf(" -X main.buildstamp=%d", buildStamp()))
	return b.String()
}

func join(fp string) string {
	return filepath.Join(packageRoot, fp)
}

func rmr(paths ...string) {
	for _, path := range paths {
		log.Println("rm -r", path)
		os.RemoveAll(path)
	}
}

func cpr(src, dist string) {
	runPrint("cp", "-rp", src, dist)
}

func cp(src, dist string) {
	runPrint("cp", "-p", src, dist)
}

func rm(dir string) {
	runPrint("rm", "-rf", dir)
}

func mkdir(dir string) {
	runPrint("mkdir", "-p", dir)
}

func mkdirs(dirs ...string) {
	for _, dir := range dirs {
		mkdir(dir)
	}
}

func clean() {
	rmr("dist", "tmp")
	rmr(filepath.Join(os.Getenv("GOPATH"), fmt.Sprintf("pkg/%s_%s/github.com/%s", goos, goarch, binaryName)))
}

func setBuildEnv() {
	os.Setenv("GOOS", goos)
	if strings.HasPrefix(goarch, "armv") {
		os.Setenv("GOARCH", "arm")
		os.Setenv("GOARM", goarch[4:])
	} else {
		os.Setenv("GOARCH", goarch)
	}
	if goarch == "386" {
		os.Setenv("GO386", "387")
	}
	if cgo != "" {
		os.Setenv("CGO_ENABLED", cgo)
	}
	if gocc != "" {
		os.Setenv("CC", gocc)
	}
	if gocxx != "" {
		os.Setenv("CXX", gocxx)
	}
}

func getGitSha() string {
	// exe cmd git rev-parse --short HEAD
	v, err := runError("git", "rev-parse", "--short", "HEAD")
	if err != nil {
		return "unknown-dev"
	}
	return string(v)
}

func buildStamp() int64 {
	// exe cmd git show -s --format=%ct
	bs, err := runError("git", "show", "-s", "--format=%ct")
	if err != nil {
		return time.Now().Unix()
	}
	s, _ := strconv.ParseInt(string(bs), 10, 64)
	return s
}

func buildArch() string {
	os := goos
	if os == "darwin" {
		os = "macosx"
	}
	return fmt.Sprintf("%s-%s", os, goarch)
}

func run(cmd string, args ...string) []byte {
	bs, err := runError(cmd, args...)
	if err != nil {
		log.Println(cmd, strings.Join(args, " "))
		log.Println(string(bs))
		log.Fatal(err)
	}
	return bytes.TrimSpace(bs)
}

func runError(cmd string, args ...string) ([]byte, error) {
	ecmd := exec.Command(cmd, args...)
	bs, err := ecmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	return bytes.TrimSpace(bs), nil
}

func runPrint(cmd string, args ...string) {
	log.Println(cmd, strings.Join(args, " "))
	ecmd := exec.Command(cmd, args...)
	ecmd.Stdout = os.Stdout
	ecmd.Stderr = os.Stderr
	err := ecmd.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func md5File(file string) error {
	fd, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fd.Close()

	h := md5.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return err
	}

	out, err := os.Create(file + ".md5")
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "%x\n", h.Sum(nil))
	if err != nil {
		return err
	}

	return out.Close()
}

func shaFilesInDist() {
	filepath.Walk("./dist", func(path string, f os.FileInfo, err error) error {
		if path == "./dist" {
			return nil
		}

		if strings.Contains(path, ".sha256") == false {
			err := shaFile(path)
			if err != nil {
				log.Printf("Failed to create sha file. error: %v\n", err)
			}
		}
		return nil
	})
}

func shaFile(file string) error {
	fd, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fd.Close()

	h := sha256.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return err
	}

	out, err := os.Create(file + ".sha256")
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "%x\n", h.Sum(nil))
	if err != nil {
		return err
	}

	return out.Close()
}
