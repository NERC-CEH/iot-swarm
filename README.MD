# Outlook Meeting Note Generator

## About

This project is a Python GUI application for generating meeting documents from calendar events on Outlook. It allows you to quickly build your meeting information without needing to copy and paste descriptions or attendees while suggesting consistent folder naming for easy organisation.

It functions by using package `win32com` to communicate with a local Outlook application. This makes it platform locked at this point and requires Outlook to be installed.

## Installation and Usage

Install to your preferred `pip` environment:

```
pip install git+https://github.com/lewis-chambers/outlook-meeting-note-builder.git
```

After installation there will be an `outlook-meeting-notes` executable in the `<python-env>/(scripts|bin)` directory that can be called to open the GUI.

![app screenshot](readme-sources/application.png)


## Roadmap

Currently the project fulfills a MVP, and has very basic functionality. Some expected future developments:

- [ ] Expand to other platforms
- [ ] Enable finer control over metadata added to document
- [ ] Remembering user config preferences
- [ ] Faster event loading (event loading is pretty slow right now)
- [ ] Implementation of common meeting templates
- [ ] Assign meeting agenda items from GUI
- [ ] Better visibility on which days have events
- [ ] Implement caching