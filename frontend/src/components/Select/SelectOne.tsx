import * as React from 'react';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select, { SelectChangeEvent } from '@mui/material/Select';

interface SelectOneProps {
    data: string[];
    title: string;
    setCurrentValue: React.Dispatch<React.SetStateAction<string>>;
}

const SelectOne: React.FC<SelectOneProps> = ({ data, title, setCurrentValue }) => {
    const [value, setValue] = React.useState('');

    const handleChange = (event: SelectChangeEvent) => {
        setValue(event.target.value);
        setCurrentValue(event.target.value);
    };

    return (
        <FormControl sx={{ m: 1, minWidth: 400 }}>
            <InputLabel id="demo-select-small-label">{title}</InputLabel>
            <Select labelId="demo-select-small-label" id="demo-select-small" value={value} label={title} onChange={handleChange}>
                {data.map((name, index) => (
                    <MenuItem key={index} value={name}>{name}</MenuItem>
                ))}
            </Select>
        </FormControl>
    );
}

export default SelectOne