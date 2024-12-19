import ButtonSend from '../Button/ButtonSend.tsx';
import Store from '../../store/Store.ts';
import * as React from 'react';
import axios from 'axios';
import { observer } from 'mobx-react-lite';
import ButtonBack from '../Button/ButtonBack.tsx';
import Input from '@mui/material/Input';
import MultipleSelect from '../Select/MultipleSelect.tsx';

const Optimization = observer(() => {
    const [activesName, setActivesName] = React.useState<string[]>([]);
    const [activesWeight, setActivesWeight] = React.useState<number[]>([]);

    function parseResultsClient(data: [{ [key: string]: string }]) {
        return data.map((item) => {
            const stocks = Object.keys(item)
                .filter((key) => key !== 'Optimization' && key !== 'Portfolio_Risk' && key !== 'Portfolio_Return')
                .map((key) => ({ name: key, value: item[key] }));
            return {
                Optimization: item.Optimization ? item.Optimization : 'оптимизация клиента',
                Portfolio_Return: item.Portfolio_Return,
                Portfolio_Risk: item.Portfolio_Risk,
                Stocks: stocks,
            };
        });
    }

    function makeOwnOptimization(activName: string[], activWeight: number[]) {
        if (activName.length !== 0) {
            console.log(activName, activWeight);
            if (activName.length < 2) {
                alert('Минимальное количество активов - 2');
                return;
            } else if (activWeight.length !== activName.length) {
                alert('Количество весов активов должно быть равно количеству активов');
                return;
            } else if (activWeight.reduce((accumulator, currentValue) => accumulator + currentValue, 0) !== 1) {
                alert('Сумма весов активов должна быть равна 1');
                return;
            } else {
                axios
                    .post('http://127.0.0.1:8000/optimize/client_port', {
                        securities: activName,
                        weights: activWeight,
                    })
                    .then((response) => {
                        Store.setResultWeight = parseResultsClient(response.data.data);
                        Store.setImageFigure = response.data.image;
                        Store.setCurrentPage = 'result';
                    })
                    .catch((error) => console.error(error));
            }
        }
    }

    return (
        <div>
            <h1 style={{ fontSize: '64px', marginBottom: '32px' }}>Введите свои данные для оптимизации</h1>
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-evenly',
                    marginBottom: '64px',
                }}>
                <MultipleSelect data={Store.getCurrencys} title="Выберите свои активы" setCurrentValue={setActivesName} />
                <Input
                    style={{ minWidth: '460px' }}
                    onChange={(e) => setActivesWeight(e.target.value.split(',').map((item) => parseFloat(item.trim())))}
                    type={'text'}
                    placeholder={'Веса активов в формате: "0.1, 0.9" - сумма должна быть 1'}
                />
            </div>
            <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '24px' }}>
                <ButtonSend title={'Рассчитать оптимизацию'} onClick={() => makeOwnOptimization(activesName, activesWeight)} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'center' }}>
                <ButtonBack
                    style={{ width: '100%', backgroundColor: 'green' }}
                    title={'Вернуться на главную'}
                    onClick={() => {
                        Store.currentPage = 'greeting';
                        Store.setResultWeight = [];
                        Store.setCurrentOptimizationType = 'free';
                        Store.setCurrencys = [];
                        Store.setCurrency = '';
                    }}
                />
            </div>
        </div>
    );
});

export default Optimization;
